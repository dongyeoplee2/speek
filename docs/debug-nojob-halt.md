# speek+ No-Job Halt Bug — Debug Log

## Problem
When the user has **no running SLURM jobs**, `speek+` renders the first frame but then **freezes**: the clock doesn't tick, keyboard navigation doesn't work, only `q` (quit) works. The app works normally when jobs are running.

## Environment
- **Terminal**: wezterm over SSH
- **Python**: 3.9.12 (anaconda3)
- **Textual**: 8.1.1
- **SLURM**: cluster with 4 GPU models (3090, A100, A6000, PRO6000)
- **OS**: Linux 4.18.0, RHEL 8

## Key Finding
The app IS running internally — a `script` capture confirmed the clock ticks every second (`:22` to `:26`). The freeze is a **rendering/display issue** specific to wezterm over SSH, not a Python crash.

## Confirmed Facts
| # | Test | Result |
|---|------|--------|
| 1 | Simple Textual app (Label + tick timer) | **Works** |
| 2 | `T(App)` with real widgets (ClusterBar, Queue, MyJobs, History, CommandBar) + full SCSS | **Works** |
| 3 | `T(App)` with real widgets + register 80+ themes + load settings | **Works** |
| 4 | `T(App)` with real widgets + full bindings + title bar + lazy tab placeholders | **Works** |
| 5 | `T(SpeekMax)` with just Label + Footer (simple compose) | **Works** |
| 6 | `T(SpeekMax)` with ClusterBar + QueueWidget | **Works** |
| 7 | `T(SpeekMax)` with ClusterBar + QueueWidget + **MyJobsWidget** | **FREEZES** |
| 8 | `T(SpeekMax)` with MyJobsWidget, ALL handlers overridden to `pass` | **FREEZES** |
| 9 | `T(SpeekMax)` with MyJobsWidget, ALL methods `delattr`'d + `_inherit_bindings=False` | **Works** |
| 10 | `script` capture of real `speek+` | Clock ticks internally (`:22`→`:26`) |
| 11 | User submits a job while `speek+` is "frozen" | **App becomes interactive immediately** |

## Textual Architecture (relevant details)

### Message dispatch walks the full MRO
Confirmed from Textual source (`_process_messages_loop`):
```python
for cls, method in self._get_dispatch_methods(handler_name, message):
    await invoke(method, message)
```
Textual calls EVERY `on_*` handler in the class hierarchy, not just the most-derived one. Overriding a handler to `pass` on a subclass does NOT prevent the parent's handler from also being called.

### Widget message loops die on exception
```python
try:
    await self._dispatch_message(message)
except Exception as error:
    self.app._handle_exception(error)
    break  # <-- widget loop dies permanently
```
Any unhandled exception in a message handler kills that widget's event loop forever. The widget stays in the DOM tree but stops processing all messages (timers, clicks, keys).

### Messages bubble up the tree
`RunningCount` from MyJobsWidget bubbles: `MyJobsWidget → Vertical → Horizontal → Screen → App`. Every widget in the chain with a matching `on_*` handler gets it called.

## Bugs Found and Fixed Along the Way

### 1. `SettingsWidget.on_select_changed` — theme crash
**Cause**: `Select` widgets fire `Changed` during mount. The handler called `self.app.theme = str(val)` before themes were registered → `InvalidThemeError` → widget loop dies.
**Fix**: Wrapped in `try/except`.

### 2. `UsersWidget.on_show` — `set_interval(count=1)`
**Cause**: `count` parameter doesn't exist in Textual 8.1.1 → `TypeError` → widget loop dies.
**Fix**: Changed to `set_timer(3, self._load)`.

### 3. `MyJobsWidget.query_one(SpeekDataTable)` — TooManyMatches
**Cause**: MyJobsWidget has TWO DataTables (current + history). `query_one(SpeekDataTable)` raises `TooManyMatches`.
**Fix**: Changed to `query_one(_MYJOBS_DT, SpeekDataTable)`.

### 4. `report_error` writes to stderr
**Cause**: `traceback.print_exc(file=sys.stderr)` inside `report_error` writes to stderr while Textual owns the terminal, corrupting the display.
**Fix**: Changed to write to `/tmp/speek_errors.log` instead.

### 5. `EventWatcher.start()` blocks main thread
**Cause**: `_query(self._user)` runs synchronously on the main thread (squeue with 10s timeout).
**Fix**: Changed to `run_worker(_seed, thread=True)`.

## Hypotheses for the No-Job Freeze

### H1: `_update_title_right` accesses `theme_variables` during mount (PARTIALLY CONFIRMED)
When MyJobsWidget posts `RunningCount(0, 0)`, `on_my_jobs_widget_running_count` calls `_update_title_right()` which accesses `self.theme_variables`. This may trigger a reentrant layout computation during the mount phase, deadlocking the compositor.

**Evidence for**:
- Removing `_update_title_right` calls (handlers only store values) still froze — but this was tested while other issues (like `@safe` stderr writing) were also present.
- The `script` capture shows the app IS ticking internally.

**Evidence against**:
- `call_later(self._update_title_right)` didn't fix it.
- `is_mounted` guard didn't fix it.

### H2: Binding conflicts between App and widget (DEBUNKED)
SpeekMax and MyJobsWidget both define bindings for keys `1`, `2`, `d`. Theory: conflicting bindings cause Footer re-render loop.

**Evidence against**: Removing conflicting bindings from App didn't fix it.

### H3: `@safe` decorator + `report_error` + `notify()` cascade (PARTIALLY CONFIRMED)
When `@safe` catches an exception, it calls `report_error` which calls `self.app.notify()`. During initialization, `notify()` creates toast widgets and triggers layout, potentially causing a cascade.

**Evidence for**: The `@safe` decorator was present on `_update_title_right`, and `_update_title_right` was called during early init.

### H4: SSH rendering flood from LoadingIndicator animation (STRONG HYPOTHESIS)
LoadingIndicators animate continuously (spinning dots). Over SSH, this floods the terminal with updates. wezterm buffers them and stalls the visual refresh.

**Evidence for**:
- `script` capture proves the app IS updating internally — the clock ticks every second.
- The user confirmed: "as soon as I submit a job, it started to be interactable" — submitting a job changes widget content, which forces wezterm to redraw.
- LoadingIndicators start visible and are only hidden after first data load completes.

**Evidence against**:
- Not yet tested: hiding LoadingIndicators by default via CSS.

### H5: wezterm SSH rendering optimization skips "unchanged" frames (STRONG HYPOTHESIS)
When there are no jobs, the empty state content doesn't change after the first frame. wezterm over SSH may optimize by not redrawing frames where the terminal content hasn't changed. The clock updates are too small (just seconds digit) to trigger a full redraw.

**Evidence for**:
- Consistent with the `script` capture (app works internally).
- Consistent with "submitting a job fixes it" (new content forces redraw).
- Local terminal users wouldn't see this issue.

## Next Steps to Try

1. **Hide LoadingIndicators by default** via CSS `display: none`. Only show them when explicitly loading.

2. **Force periodic full redraw**. Add a timer that calls `self.refresh(repaint=True)` every 2 seconds to force wezterm to redraw.

3. **Test `TERM=xterm-256color speek+`**. The SSH session has `TERM=dumb` which may cause wezterm to skip redraws.

4. **Test with a different terminal** over SSH (`alacritty`, `kitty`, plain `xterm`) to confirm wezterm-specific.

5. **Move `_update_title_right` to a polling timer** instead of calling it from message handlers. This avoids reentrant layout during mount.

6. **Set `_is_ready` flag after `App.Ready`** and guard all handlers that touch layout/theme.

## Files Modified During Debugging
- `speek/speek_max/app.py` — handlers, bindings, on_mount, _SpeekScreen, lazy tabs
- `speek/speek_max/_utils.py` — `report_error`, `@safe` decorator, `_log_error`
- `speek/speek_max/widgets/settings_widget.py` — `on_select_changed` guard
- `speek/speek_max/widgets/users_widget.py` — `on_show` fix
- `speek/speek_max/widgets/my_jobs_widget.py` — `query_one` fix
- `speek/speek_max/widgets/queue_widget.py` — `on_show`/`on_mount` restructure
- `speek/speek_max/widgets/history_widget.py` — empty state handling
- `speek/speek_max/event_watcher.py` — async `start()`
- `speek/speek_max/speek_max.scss` — LoadingIndicator, empty-state CSS
