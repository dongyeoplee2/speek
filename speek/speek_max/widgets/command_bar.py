"""command_bar.py — Shell-like command bar with history and autocomplete."""
from __future__ import annotations

import json
import shlex
import subprocess
from pathlib import Path
from typing import List

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Button, Input, Label, OptionList, Static
from textual.widgets.option_list import Option

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

_CONFIG_DIR = Path.home() / '.config' / 'speek-max'
_HISTORY_PATH = _CONFIG_DIR / 'command_history.json'
_COMMANDS_PATH = _CONFIG_DIR / 'commands.yaml'
_MAX_HISTORY = 200
_SUGGESTIONS_ID = '#cmd-suggestions'
_INPUT_ID = '#cmd-input'

# Shared description strings
_DESC_MY_JOBS = 'show only my jobs'
_DESC_BY_USER = 'filter by user'
_DESC_LONG = 'long format'
_DESC_JOB_ID = 'specific job ID'
_DESC_BY_PART = 'filter by partition'
_GRES_GPU = '--gres=gpu:'

# SLURM commands (used for context-aware flag completions)
_SLURM_CMDS = frozenset({
    'sbatch', 'scancel', 'squeue', 'scontrol', 'sinfo', 'sacct', 'sprio', 'srun',
})

# Built-in suggestions shown on empty tab
_BUILTIN_SUGGESTIONS: list[tuple[str, str]] = [
    # SLURM
    ('squeue --me',            'show my jobs'),
    ('squeue --me -l',         'my jobs (long)'),
    ('sinfo -N -l',            'node list (long)'),
    ('sacct --me -S now-1day', 'my history (1 day)'),
    ('scontrol show job ',     'inspect a job'),
    ('sbatch ',                'submit a batch job'),
    ('scancel ',               'cancel jobs'),
    # Shell
    ('ls',                     'list files'),
    ('ls -la',                 'list all files (long)'),
    ('cd ',                    'change directory'),
    ('pwd',                    'print working directory'),
    ('cat ',                   'print file contents'),
    ('head ',                  'first lines of file'),
    ('tail ',                  'last lines of file'),
    ('tail -f ',               'follow file'),
    ('grep ',                  'search in files'),
    # Git
    ('git status',             'working tree status'),
    ('git log --oneline -20',  'recent commits'),
    ('git branch',             'list branches'),
    ('git diff',               'show changes'),
    ('git pull',               'pull from remote'),
    # Python
    ('python --version',       'python version'),
    ('pip list',               'installed packages'),
    ('conda env list',         'conda environments'),
    ('nvidia-smi',             'GPU status'),
    ('which python',           'python path'),
]

# Per-command flag suggestions: (flag, description)
_CMD_FLAGS: dict[str, list[tuple[str, str]]] = {
    'sbatch': [
        ('-p ',              _DESC_BY_PART),
        ('-N ',              'number of nodes'),
        ('-n ',              'number of tasks'),
        (_GRES_GPU,          'GPU resource (e.g. gpu:a100:2)'),
        ('-t ',              'time limit (e.g. 1:00:00)'),
        ('-J ',              'job name'),
        ('-o ',              'stdout file path'),
        ('-e ',              'stderr file path'),
        ('--mem=',           'memory per node (e.g. 32G)'),
        ('--mem-per-cpu=',   'memory per CPU'),
        ('--cpus-per-task=', 'CPUs per task'),
        ('--ntasks-per-node=', 'tasks per node'),
        ('--array=',         'job array (e.g. 0-9)'),
        ('--wrap=',          'wrap command in script'),
        ('--nodelist=',      'specific nodes'),
        ('--exclude=',       'exclude nodes'),
        ('--constraint=',    'node constraint'),
        ('--qos=',           'quality of service'),
        ('-A ',              'account/project'),
        ('--dependency=',    'dependency (e.g. afterok:123)'),
        ('--mail-type=',     'email events (END, FAIL, ALL)'),
        ('--export=',        'env vars (ALL, NONE)'),
    ],
    'scancel': [
        ('--me',       'cancel all my jobs'),
        ('-u ',        'cancel by user'),
        ('-p ',        'cancel by partition'),
        ('-t ',        'cancel by state'),
        ('-n ',        'cancel by job name'),
        ('--signal=',  'send signal instead of kill'),
    ],
    'squeue': [
        ('--me',       _DESC_MY_JOBS),
        ('-u ',        _DESC_BY_USER),
        ('-p ',        _DESC_BY_PART),
        ('-t ',        'filter by state (R, PD, F)'),
        ('-l',         _DESC_LONG),
        ('-o ',        'custom output format'),
        ('-S ',        'sort by field'),
        ('-j ',        _DESC_JOB_ID),
        ('-w ',        'filter by node'),
    ],
    'scontrol': [
        ('show job ',       'show job details'),
        ('show node ',      'show node details'),
        ('show partition ', 'show partition details'),
        ('show config',     'show SLURM config'),
        ('hold ',           'hold a job'),
        ('release ',        'release a held job'),
        ('requeue ',        'requeue a job'),
    ],
    'sinfo': [
        ('-N',         'node-oriented format'),
        ('-l',         _DESC_LONG),
        ('-p ',        _DESC_BY_PART),
        ('-n ',        'filter by node'),
        ('-t ',        'filter by state'),
        ('-o ',        'custom output format'),
    ],
    'sacct': [
        ('--me',              _DESC_MY_JOBS),
        ('-u ',               _DESC_BY_USER),
        ('-S ',               'start time (e.g. now-1day)'),
        ('-E ',               'end time'),
        ('-j ',               _DESC_JOB_ID),
        ('-o ',               'output fields'),
        ('-l',                _DESC_LONG),
        ('-X',                'only allocations (no steps)'),
        ('-s ',               'filter by state'),
        ('--parsable2',       'parsable output'),
    ],
    'sprio': [
        ('--me',   _DESC_MY_JOBS),
        ('-u ',    _DESC_BY_USER),
        ('-l',     _DESC_LONG),
        ('-j ',    _DESC_JOB_ID),
    ],
    'srun': [
        ('-p ',              _DESC_BY_PART),
        ('-N ',              'number of nodes'),
        ('-n ',              'number of tasks'),
        (_GRES_GPU,          'GPU resource'),
        ('-t ',              'time limit'),
        ('--mem=',           'memory per node'),
        ('--cpus-per-task=', 'CPUs per task'),
        ('--pty',            'pseudo-terminal (interactive)'),
        ('bash',             'start bash shell'),
    ],
}

# Flags that trigger dynamic value completion: flag_prefix → resolver_name
_DYNAMIC_FLAGS: dict[str, str] = {
    '-p ': 'partitions',
    '--partition=': 'partitions',
    '-w ': 'nodes',
    '--nodelist=': 'nodes',
    '-n ': 'nodes',  # for sinfo -n
    _GRES_GPU: 'gpu_types',
    '-t ': 'states',
    '--state=': 'states',
    '--states=': 'states',
}

_SLURM_STATES = ['RUNNING', 'PENDING', 'COMPLETED', 'FAILED', 'TIMEOUT',
                 'CANCELLED', 'OUT_OF_MEMORY', 'SUSPENDED', 'PREEMPTED']


import re as _re

# Patterns that indicate auto-generated sub-job scripts (not user scripts)
_SUBJOB_RE = _re.compile(
    r'\.submission_file_|/slurm_output/\.|/tmp/|\.tmp\.|_spawned_|_sub_\d|'
    r'\.slurm\.[0-9a-f]{8,}|/\.hydra/',
    _re.IGNORECASE,
)


def _is_user_script(path: str) -> bool:
    """Return True if the script looks like a user-written script, not auto-generated."""
    if not path:
        return False
    if path.startswith('/bin/') or path.startswith('/usr/'):
        return False
    if _SUBJOB_RE.search(path):
        return False
    # Hidden files in output dirs are usually auto-generated
    basename = path.rsplit('/', 1)[-1] if '/' in path else path
    if basename.startswith('.') and not basename.startswith('./'):
        return False
    return True


def _fetch_recent_scripts() -> List[tuple[str, str]]:
    """Collect script paths from running + recent jobs via scontrol and sacct.

    Filters out auto-generated sub-job scripts.
    Returns list of (script_path, description) tuples.
    """
    scripts: dict[str, str] = {}  # path → description

    # 1. Running/pending jobs (my jobs only): squeue → job IDs, then scontrol
    import getpass
    user = getpass.getuser()
    try:
        out = subprocess.check_output(
            ['squeue', '-u', user, '-h', '-o', '%i'],
            text=True, stderr=subprocess.DEVNULL, timeout=5,
        )
        my_jids = [j.strip() for j in out.splitlines() if j.strip()]
        if my_jids:
            out2 = subprocess.check_output(
                ['scontrol', 'show', 'job', ','.join(my_jids[:50]), '--oneliner'],
                text=True, stderr=subprocess.DEVNULL, timeout=5,
            )
            for line in out2.splitlines():
                cmd = jname = partition = ''
                for token in line.split():
                    if token.startswith('Command='):
                        cmd = token.split('=', 1)[1]
                    elif token.startswith('JobName='):
                        jname = token.split('=', 1)[1]
                    elif token.startswith('Partition='):
                        partition = token.split('=', 1)[1]
                if _is_user_script(cmd):
                    desc = f'{jname} [{partition}]' if jname else 'running'
                    scripts.setdefault(cmd, desc)
    except Exception:
        pass

    # 2. Recent completed jobs: sacct SubmitLine → last positional arg is script
    try:
        out = subprocess.check_output(
            ['sacct', '--me', '-S', 'now-7day', '--parsable2', '--noheader',
             '--format=SubmitLine,JobName,Partition', '-X'],
            text=True, stderr=subprocess.DEVNULL, timeout=5,
        )
        for line in out.splitlines():
            parts = (line.split('|') + ['', '', ''])[:3]
            submit_line, jname, partition = parts
            if not submit_line.strip():
                continue
            try:
                tokens = shlex.split(submit_line)
            except ValueError:
                tokens = submit_line.split()
            # Find the script path: last positional arg (not a flag, not "sbatch")
            script = ''
            for t in reversed(tokens):
                if not t.startswith('-') and t != 'sbatch':
                    script = t
                    break
            if _is_user_script(script):
                desc = f'{jname} [{partition}]' if jname else 'recent'
                scripts.setdefault(script, desc)
    except Exception:
        pass

    return list(scripts.items())


def _load_json_list(path: Path) -> list:
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_json_list(path: Path, data: list) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data))
    except Exception:
        pass


def _load_user_commands(path: Path) -> dict:
    """Load user commands from YAML.

    Format:
        aliases:
          gpu4: "sbatch --gres=gpu:4"
          myq: "squeue --me"
        commands:
          - "squeue --me --format='%.18i %.9P %.30j'"
          - "sinfo -N -l"
    """
    if not _HAS_YAML or not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text()) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


class CommandBar(Widget):
    """Shell-like command bar with history and tab completion."""

    BORDER_TITLE = 'Shell'

    BINDINGS = [
        Binding('escape', 'blur_input', 'Unfocus', show=False),
    ]

    class CommandExecuted(Message):
        """Posted after a command finishes."""
        def __init__(self, command: str, output: str, success: bool) -> None:
            super().__init__()
            self.command = command
            self.output = output
            self.success = success

    _MAX_SUGGESTIONS = 8

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._history: List[str] = _load_json_list(_HISTORY_PATH)
        self._history_index: int = -1
        self._saved_input: str = ''
        self._user_cmds: dict = _load_user_commands(_COMMANDS_PATH)
        self._completions: list[str] = []
        self._completion_index: int = -1
        self._suggestions_visible: bool = False

    def compose(self) -> ComposeResult:
        yield OptionList(id='cmd-suggestions')
        with Horizontal(id='cmd-bar'):
            yield Label('$', id='cmd-prompt')
            yield Input(placeholder='type command or : to focus', id='cmd-input')
            yield Static('', id='cmd-status', markup=True)
            yield Button('⎘', id='cmd-copy-btn')
            yield Button('📋', id='cmd-paste-btn')
            yield Button('⏎', id='cmd-submit-btn')

    def on_mount(self) -> None:
        self._user_cmds = _load_user_commands(_COMMANDS_PATH)
        self._hide_suggestions()

    # ── Suggestion dropdown ────────────────────────────────────────────────

    def _show_suggestions(self, items: List[tuple[str, str]]) -> None:
        """Show suggestion dropdown. Items are (value, description) tuples."""
        ol = self.query_one(_SUGGESTIONS_ID, OptionList)
        ol.clear_options()
        self._completions = []
        for value, desc in items[:self._MAX_SUGGESTIONS]:
            # Show "value  — description" in the dropdown
            label = f'{value}  [dim]— {desc}[/dim]' if desc else value
            ol.add_option(Option(label, id=str(len(self._completions))))
            self._completions.append(value)
        if items:
            ol.display = True
            ol.highlighted = 0
            self._suggestions_visible = True
        else:
            self._hide_suggestions()

    def _hide_suggestions(self) -> None:
        try:
            ol = self.query_one(_SUGGESTIONS_ID, OptionList)
            ol.display = False
            ol.clear_options()
        except Exception:
            pass
        self._suggestions_visible = False

    def _accept_suggestion(self) -> None:
        ol = self.query_one(_SUGGESTIONS_ID, OptionList)
        idx = ol.highlighted
        if idx is not None and 0 <= idx < len(self._completions):
            inp = self.query_one(_INPUT_ID, Input)
            inp.value = self._completions[idx]
            inp.cursor_position = len(inp.value)
        self._hide_suggestions()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id == _SUGGESTIONS_ID:
            idx = event.option_index
            if 0 <= idx < len(self._completions):
                inp = self.query_one(_INPUT_ID, Input)
                inp.value = self._completions[idx]
                inp.cursor_position = len(inp.value)
                self._hide_suggestions()
                inp.focus()

    # ── Button handlers ─────────────────────────────────────────────────────

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'cmd-copy-btn':
            self._copy_last_output()
        elif event.button.id == 'cmd-paste-btn':
            self._paste_clipboard()
        elif event.button.id == 'cmd-submit-btn':
            inp = self.query_one(_INPUT_ID, Input)
            raw = inp.value.strip()
            if raw:
                self._hide_suggestions()
                if raw in ('exit', 'quit', 'q'):
                    self.app.exit()
                    return
                self._execute(raw)
                inp.value = ''
                self._history_index = -1

    def _copy_last_output(self) -> None:
        """Copy the last command output to clipboard."""
        if hasattr(self, '_last_output') and self._last_output:
            try:
                import pyperclip
                pyperclip.copy(self._last_output)
                self._show_status('[dim]copied[/dim]')
            except Exception:
                self._show_status('[dim]copy failed[/dim]')
        else:
            self._show_status('[dim]no output[/dim]')

    def _paste_clipboard(self) -> None:
        """Paste clipboard content into the input."""
        try:
            import pyperclip
            text = pyperclip.paste()
            if text:
                inp = self.query_one(_INPUT_ID, Input)
                inp.value = inp.value + text.splitlines()[0]  # first line only
                inp.cursor_position = len(inp.value)
                inp.focus()
        except Exception:
            self._show_status('[dim]paste failed[/dim]')

    # ── Input handling ─────────────────────────────────────────────────────

    def on_input_changed(self, event: Input.Changed) -> None:
        """Auto-show suggestions as the user types."""
        if event.input.id != 'cmd-input':
            return
        raw = event.value
        if not raw.strip():
            self._hide_suggestions()
            return
        # Pass raw value (preserve trailing space so "sbatch " triggers arg completions)
        candidates = self._get_completions(raw)
        # Don't show if the only match is exactly what's typed
        if candidates and not (len(candidates) == 1 and candidates[0][0] == raw):
            self._show_suggestions(candidates)
        else:
            self._hide_suggestions()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != 'cmd-input':
            return
        # If suggestions visible, accept the highlighted one first
        if self._suggestions_visible:
            self._accept_suggestion()
            return
        raw = event.value.strip()
        if not raw:
            return
        self._hide_suggestions()
        if raw in ('exit', 'quit', 'q'):
            self.app.exit()
            return
        self._execute(raw)
        event.input.value = ''
        self._history_index = -1

    def on_key(self, event) -> None:
        inp = self.query_one(_INPUT_ID, Input)
        if not inp.has_focus:
            return

        if event.key == 'escape':
            event.prevent_default()
            self._hide_suggestions()
            self.screen.focus_next()
            return

        if event.key == 'tab':
            event.prevent_default()
            event.stop()  # Don't let App intercept Tab for focus cycling
            self._do_tab_complete(inp)
            return

        if self._suggestions_visible:
            ol = self.query_one(_SUGGESTIONS_ID, OptionList)
            if event.key == 'down':
                event.prevent_default()
                if ol.highlighted is not None and ol.highlighted < ol.option_count - 1:
                    ol.highlighted = ol.highlighted + 1
                return
            elif event.key == 'up':
                event.prevent_default()
                if ol.highlighted is not None and ol.highlighted > 0:
                    ol.highlighted = ol.highlighted - 1
                else:
                    self._hide_suggestions()
                    self._history_up(inp)
                return
            elif event.key == 'enter':
                event.prevent_default()
                self._accept_suggestion()
                return

        if event.key == 'up':
            event.prevent_default()
            self._history_up(inp)
        elif event.key == 'down':
            event.prevent_default()
            self._history_down(inp)

    def _do_tab_complete(self, inp: Input) -> None:
        """Terminal-style tab completion: fill common prefix, then show options."""
        raw = inp.value
        candidates = self._get_completions(raw)
        if not candidates:
            return

        values = [c[0] for c in candidates]

        if len(values) == 1:
            # Single match — complete it fully
            inp.value = values[0]
            inp.cursor_position = len(inp.value)
            self._hide_suggestions()
            return

        # Multiple matches — find longest common prefix
        prefix = values[0]
        for v in values[1:]:
            while not v.startswith(prefix):
                prefix = prefix[:-1]
                if not prefix:
                    break

        if prefix and prefix != raw:
            # Extend to common prefix
            inp.value = prefix
            inp.cursor_position = len(inp.value)

        # Show all options
        self._show_suggestions(candidates)

    def action_blur_input(self) -> None:
        self._hide_suggestions()
        self.screen.focus_next()

    # ── History navigation ─────────────────────────────────────────────────

    def _history_up(self, inp: Input) -> None:
        if not self._history:
            return
        if self._history_index == -1:
            self._saved_input = inp.value
        if self._history_index < len(self._history) - 1:
            self._history_index += 1
            inp.value = self._history[self._history_index]
            inp.cursor_position = len(inp.value)

    def _history_down(self, inp: Input) -> None:
        if self._history_index > 0:
            self._history_index -= 1
            inp.value = self._history[self._history_index]
            inp.cursor_position = len(inp.value)
        elif self._history_index == 0:
            self._history_index = -1
            inp.value = self._saved_input
            inp.cursor_position = len(inp.value)

    def _push_history(self, cmd: str) -> None:
        # Deduplicate: remove if already exists
        if cmd in self._history:
            self._history.remove(cmd)
        self._history.insert(0, cmd)
        self._history = self._history[:_MAX_HISTORY]
        _save_json_list(_HISTORY_PATH, self._history)

    # ── Tab completion ─────────────────────────────────────────────────────

    def _tab_complete(self, inp: Input) -> None:
        if self._completions and self._completion_index >= 0:
            # Cycle through existing completions
            self._completion_index = (
                (self._completion_index + 1) % len(self._completions)
            )
            inp.value = self._completions[self._completion_index]
            inp.cursor_position = len(inp.value)
            return

        prefix = inp.value.strip()
        candidates = self._get_completions(prefix)
        if not candidates:
            return
        self._completions = candidates
        self._completion_index = 0
        inp.value = candidates[0]
        inp.cursor_position = len(inp.value)
        if len(candidates) > 1:
            self._show_status(f'{len(candidates)} matches (Tab to cycle)')

    # Type alias: (completion_value, description)
    Suggestion = tuple[str, str]

    def _get_completions(self, prefix: str) -> List[Suggestion]:
        parts = prefix.split()
        cmd_name = parts[0].lower() if parts else ''

        # If we have a command + space, do context-aware completion
        if cmd_name and (len(parts) > 1 or prefix.endswith(' ')):
            # SLURM commands get flag completions
            if cmd_name in _SLURM_CMDS:
                return self._get_arg_completions(prefix, cmd_name, parts)
            # All other commands get path + history completions
            return self._get_generic_arg_completions(prefix, cmd_name, parts)

        # Otherwise: complete the command itself
        return self._get_cmd_completions(prefix)

    def _get_cmd_completions(self, prefix: str) -> List[Suggestion]:
        """Complete the command name (no args yet)."""
        candidates: List[CommandBar.Suggestion] = []
        pfx = prefix.lower()
        seen: set[str] = set()

        def _add(val: str, desc: str) -> None:
            if val not in seen:
                seen.add(val)
                candidates.append((val, desc))

        # User aliases
        for name, cmd in self._user_cmds.get('aliases', {}).items():
            if name.lower().startswith(pfx):
                _add(cmd if isinstance(cmd, str) else name, f'alias: {name}')

        # User saved commands
        for cmd in self._user_cmds.get('commands', []):
            if isinstance(cmd, str) and cmd.lower().startswith(pfx):
                _add(cmd, 'saved command')

        # Built-in suggestions
        for cmd, desc in _BUILTIN_SUGGESTIONS:
            if cmd.lower().startswith(pfx):
                _add(cmd, desc)

        # History matches
        for cmd in self._history:
            if cmd.lower().startswith(pfx):
                _add(cmd, 'history')

        return candidates

    def _get_arg_completions(
        self, full: str, cmd: str, parts: List[str],
    ) -> 'List[CommandBar.Suggestion]':
        """Complete flags, values, and file paths after a known command."""
        cur = '' if full.endswith(' ') else (parts[-1] if parts else '')
        base = full[:len(full) - len(cur)]
        out: list[CommandBar.Suggestion] = []
        seen: set[str] = set()

        def _add(val: str, desc: str) -> None:
            if val not in seen:
                seen.add(val)
                out.append((val, desc))

        # 1. Try dynamic value completion (e.g. after "-p " → partitions)
        dyn = self._try_dynamic_completion(full, parts, cur, base)
        if dyn:
            return dyn

        # 2. Recent scripts / local .sh files for sbatch/srun
        self._add_script_suggestions(cmd, cur, base, _add)

        # 3. Flag suggestions
        self._add_flag_suggestions(cmd, cur, full, base, _add)

        # 4. Path completion for non-flag tokens
        if cur and not cur.startswith('-'):
            for val, desc in self._path_completions(base, cur):
                _add(val, desc)

        # 5. History matches
        for h in self._history:
            if h.startswith(cmd) and h.startswith(full) and h != full:
                _add(h, 'history')

        return out

    def _try_dynamic_completion(
        self, full: str, parts: List[str], cur: str, base: str,
    ) -> 'List[CommandBar.Suggestion] | None':
        """Check if we're completing a flag value (partition, node, etc.)."""
        # After "cmd -p " → previous word is the flag
        dynamic_key = None
        if full.endswith(' ') and len(parts) >= 2:
            dynamic_key = parts[-1] + ' '
        elif not full.endswith(' ') and len(parts) >= 3:
            dynamic_key = parts[-2] + ' '

        if dynamic_key and dynamic_key in _DYNAMIC_FLAGS:
            resolver = _DYNAMIC_FLAGS[dynamic_key]
            return [
                (base + v, resolver)
                for v in self._resolve_dynamic(resolver)
                if v.lower().startswith(cur.lower())
            ]

        # --flag=value pattern (e.g. "--partition=gp")
        for dyn_flag, resolver in _DYNAMIC_FLAGS.items():
            if not dyn_flag.endswith('='):
                continue
            flag_stem = dyn_flag.rstrip('= ')
            if not cur.startswith(flag_stem):
                continue
            eq_pos = cur.find('=')
            if eq_pos < 0:
                continue
            val_part = cur[eq_pos + 1:].lower()
            prefix = cur[:eq_pos + 1]
            result = [
                (base + prefix + v, resolver)
                for v in self._resolve_dynamic(resolver)
                if v.lower().startswith(val_part)
            ]
            if result:
                return result

        return None

    def _get_generic_arg_completions(
        self, full: str, cmd: str, parts: List[str],
    ) -> 'List[CommandBar.Suggestion]':
        """Path + history completions for non-SLURM commands (git, ls, python, etc.)."""
        out: list[CommandBar.Suggestion] = []
        seen: set[str] = set()
        cur = '' if full.endswith(' ') else (parts[-1] if parts else '')
        base = full[:len(full) - len(cur)]

        def _add(val: str, desc: str) -> None:
            if val not in seen:
                seen.add(val)
                out.append((val, desc))

        # Path completion
        if cur and not cur.startswith('-'):
            for val, desc in self._path_completions(base, cur):
                _add(val, desc)
        elif not cur:
            # Show current dir contents
            for val, desc in self._path_completions(base, '.'):
                _add(val, desc)

        # History matches
        for h in self._history:
            if h.startswith(cmd) and h.startswith(full) and h != full:
                _add(h, 'history')

        return out

    def _add_script_suggestions(
        self, cmd: str, cur: str, base: str, _add,
    ) -> None:
        """Add recent scripts from SLURM jobs and local .sh/.py files."""
        if cur or cmd not in ('sbatch', 'srun'):
            return
        # Scripts from running + recent SLURM jobs (cached)
        for path, desc in self._get_cached_scripts():
            _add(base + path, desc)
        # Local .sh/.py files
        for val, desc in self._path_completions(base, '.'):
            if val.endswith(('.sh', '.py', '/')):
                _add(val, desc)

    def _add_flag_suggestions(
        self, cmd: str, cur: str, full: str, base: str, _add,
    ) -> None:
        """Add matching flag suggestions for the given command."""
        cur_lower = cur.lower()
        for flag, desc in _CMD_FLAGS.get(cmd, []):
            if flag.lower().startswith(cur_lower) and flag.rstrip('= ') not in full:
                _add(base + flag, desc)

    def _resolve_dynamic(self, resolver: str) -> List[str]:
        """Resolve dynamic values (partitions, nodes, GPU types, states)."""
        if resolver == 'partitions':
            try:
                from speek.speek_max.slurm import get_partitions
                return get_partitions()
            except Exception:
                return []
        elif resolver == 'nodes':
            try:
                from speek.speek_max.slurm import parse_nodes
                return [r[0] for r in parse_nodes()][:30]
            except Exception:
                return []
        elif resolver == 'gpu_types':
            try:
                from speek.speek_max.slurm import fetch_cluster_stats
                stats = fetch_cluster_stats()
                return sorted(stats.keys()) if stats else []
            except Exception:
                return []
        elif resolver == 'states':
            return _SLURM_STATES
        return []

    def _path_completions(
        self, base: str, partial: str,
    ) -> 'List[CommandBar.Suggestion]':
        """Complete file/directory paths like a real terminal.

        - If one match: complete it fully (append / for dirs)
        - If multiple matches with common prefix: complete to common prefix
        - Then list all options
        """
        results: list[CommandBar.Suggestion] = []
        try:
            p = Path(partial).expanduser()
            if partial.endswith('/'):
                parent, glob_pat = p, '*'
            else:
                parent, glob_pat = p.parent, p.name + '*'
            if not parent.is_dir():
                return results

            matches = sorted(parent.glob(glob_pat))[:30]
            if not matches:
                return results

            # Build display entries
            for match in matches:
                full = str(match)
                if match.is_dir():
                    full += '/'
                kind = 'dir' if match.is_dir() else match.suffix or 'file'
                results.append((base + full, kind))

        except Exception:
            pass
        return results

    _script_cache: 'List[tuple[str, str]] | None' = None
    _script_cache_time: float = 0

    def _get_cached_scripts(self) -> 'List[tuple[str, str]]':
        """Get script paths from SLURM jobs, cached for 60s."""
        import time
        now = time.monotonic()
        if self._script_cache is None or now - self._script_cache_time > 60:
            self._script_cache = _fetch_recent_scripts()
            # Also add scripts from command history
            seen = {p for p, _ in self._script_cache}
            for h in self._history:
                if not h.startswith('sbatch '):
                    continue
                for token in h.split()[1:]:
                    if not token.startswith('-') and token not in seen:
                        if '/' in token or token.endswith('.sh'):
                            self._script_cache.append((token, 'history'))
                            seen.add(token)
                        break
            self._script_cache_time = now
        return self._script_cache

    # ── Command execution ──────────────────────────────────────────────────

    _cwd: str = ''  # tracked working directory; empty = inherit

    def _execute(self, raw: str) -> None:
        # Expand user alias
        first_word = raw.split()[0] if raw.split() else ''
        aliases = self._user_cmds.get('aliases', {})
        if first_word in aliases:
            expanded = aliases[first_word]
            rest = raw[len(first_word):].strip()
            raw = f'{expanded} {rest}'.strip() if rest else expanded

        self._push_history(raw)

        # Handle cd specially (can't change cwd from a subprocess)
        if raw.strip() == 'cd' or raw.strip().startswith('cd '):
            self._handle_cd(raw.strip())
            return

        self._show_status('[dim]running…[/dim]')
        cwd = self._cwd or None

        def _run():
            try:
                out = subprocess.check_output(
                    raw, shell=True, text=True,
                    stderr=subprocess.STDOUT, timeout=60,
                    cwd=cwd,
                )
                self.app.call_from_thread(self._on_success, raw, out.strip())
            except subprocess.CalledProcessError as e:
                msg = (e.output or str(e)).strip()
                self.app.call_from_thread(self._on_error, raw, msg)
            except subprocess.TimeoutExpired:
                self.app.call_from_thread(
                    self._on_error, raw, 'Command timed out (60s)')

        self.run_worker(_run, thread=True, group='cmd-exec')

    def _handle_cd(self, raw: str) -> None:
        """Handle cd command by updating tracked cwd."""
        import os
        parts = raw.split(maxsplit=1)
        target = parts[1].strip() if len(parts) > 1 else os.path.expanduser('~')
        target = os.path.expanduser(target)
        if not os.path.isabs(target):
            base = self._cwd or os.getcwd()
            target = os.path.normpath(os.path.join(base, target))
        if os.path.isdir(target):
            self._cwd = target
            self._show_status(f'[dim]{target}[/dim]')
            # Update prompt to show cwd
            try:
                home = os.path.expanduser('~')
                display = target.replace(home, '~') if target.startswith(home) else target
                self.query_one('#cmd-prompt', Label).update(f'{display} $ ')
            except Exception:
                pass
        else:
            self._show_status(f'[bold red]cd: {target}: not a directory[/bold red]')

    def _on_success(self, cmd: str, output: str) -> None:
        self._last_output = output
        lines = output.splitlines()
        if len(lines) <= 5:
            self.app.notify(output or '(no output)', title=cmd.split()[0])
        else:
            # Show first 3 lines + count
            preview = '\n'.join(lines[:3])
            self.app.notify(
                f'{preview}\n… ({len(lines)} lines total)',
                title=cmd.split()[0],
                timeout=10,
            )
        self._show_status(f'[dim]✓ {cmd.split()[0]}[/dim]')
        self.post_message(self.CommandExecuted(cmd, output, success=True))

    def _on_error(self, cmd: str, msg: str) -> None:
        self.app.notify(msg, title=f'{cmd.split()[0]} failed', severity='error')
        self._show_status(f'[bold red]✗ {cmd.split()[0]}[/bold red]')
        self.post_message(self.CommandExecuted(cmd, msg, success=False))

    def _show_status(self, text: str) -> None:
        try:
            self.query_one('#cmd-status', Static).update(text)
        except Exception:
            pass

    # ── Public API ─────────────────────────────────────────────────────────

    def reload_user_commands(self) -> None:
        """Reload user commands from YAML."""
        self._user_cmds = _load_user_commands(_COMMANDS_PATH)
