"""themes.py — speek-max built-in and user themes.

User themes can be added as YAML files in ~/.config/speek-max/themes/.
YAML format:

    name: my-theme
    primary: "#61afef"
    secondary: "#c678dd"
    accent: "#56b6c2"
    background: "#282c34"
    surface: "#353b45"
    panel: "#3e4451"
    warning: "#d19a66"
    error: "#e06c75"
    success: "#98c379"
    dark: true
"""
from __future__ import annotations

import logging
from pathlib import Path

from textual.theme import Theme as TextualTheme

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

log = logging.getLogger(__name__)

_USER_THEME_DIR = Path.home() / '.config' / 'speek-max' / 'themes'

# Keys allowed in user theme YAML
_THEME_COLOR_KEYS = frozenset({
    'primary', 'secondary', 'background', 'surface', 'panel',
    'warning', 'error', 'success', 'accent',
})
_THEME_META_KEYS = frozenset({'name', 'dark', 'author', 'description', 'homepage'})
_THEME_VAR_PREFIX = 'var-'  # var-footer-background → variables["footer-background"]

SPEEK_THEMES: dict[str, TextualTheme] = {
    "galaxy": TextualTheme(
        name="galaxy",
        primary="#C45AFF",
        secondary="#a684e8",
        warning="#FFD700",
        error="#FF4500",
        success="#00FA9A",
        accent="#FF69B4",
        background="#0F0F1F",
        surface="#1E1E3F",
        panel="#2D2B55",
        dark=True,
        variables={
            "input-cursor-background": "#C45AFF",
            "footer-background": "transparent",
        },
    ),
    "nebula": TextualTheme(
        name="nebula",
        primary="#4A9CFF",
        secondary="#66D9EF",
        warning="#FFB454",
        error="#FF5555",
        success="#50FA7B",
        accent="#FF79C6",
        surface="#193549",
        panel="#1F4662",
        background="#0D2137",
        dark=True,
        variables={
            "input-selection-background": "#4A9CFF 35%",
        },
    ),
    "sunset": TextualTheme(
        name="sunset",
        primary="#FF7E5F",
        secondary="#FEB47B",
        warning="#FFD93D",
        error="#FF5757",
        success="#98D8AA",
        accent="#B983FF",
        background="#2B2139",
        surface="#362C47",
        panel="#413555",
        dark=True,
        variables={
            "input-cursor-background": "#FF7E5F",
            "input-selection-background": "#FF7E5F 35%",
            "footer-background": "transparent",
        },
    ),
    "aurora": TextualTheme(
        name="aurora",
        primary="#45FFB3",
        secondary="#A1FCDF",
        accent="#DF7BFF",
        warning="#FFE156",
        error="#FF6B6B",
        success="#64FFDA",
        background="#0A1A2F",
        surface="#142942",
        panel="#1E3655",
        dark=True,
        variables={
            "input-cursor-background": "#45FFB3",
            "input-selection-background": "#45FFB3 35%",
            "footer-background": "transparent",
        },
    ),
    "nautilus": TextualTheme(
        name="nautilus",
        primary="#0077BE",
        secondary="#20B2AA",
        warning="#FFD700",
        error="#FF6347",
        success="#32CD32",
        accent="#FF8C00",
        background="#001F3F",
        surface="#003366",
        panel="#005A8C",
        dark=True,
    ),
    "cobalt": TextualTheme(
        name="cobalt",
        primary="#334D5C",
        secondary="#66B2FF",
        warning="#FFAA22",
        error="#E63946",
        success="#4CAF50",
        accent="#D94E64",
        surface="#27343B",
        panel="#2D3E46",
        background="#1F262A",
        dark=True,
        variables={
            "input-selection-background": "#4A9CFF 35%",
        },
    ),
    "twilight": TextualTheme(
        name="twilight",
        primary="#367588",
        secondary="#5F9EA0",
        warning="#FFD700",
        error="#FF6347",
        success="#00FA9A",
        accent="#FF7F50",
        background="#191970",
        surface="#3B3B6D",
        panel="#4C516D",
        dark=True,
    ),
    "hacker": TextualTheme(
        name="hacker",
        primary="#00FF00",
        secondary="#3A9F3A",
        warning="#00FF66",
        error="#FF0000",
        success="#00DD00",
        accent="#00FF33",
        background="#000000",
        surface="#0A0A0A",
        panel="#111111",
        dark=True,
    ),
    "manuscript": TextualTheme(
        name="manuscript",
        primary="#2C4251",
        secondary="#6B4423",
        accent="#8B4513",
        warning="#B4846C",
        error="#A94442",
        success="#2D5A27",
        background="#F5F1E9",
        surface="#EBE6D9",
        panel="#E0DAC8",
        dark=False,
        variables={
            "input-cursor-background": "#2C4251",
            "input-selection-background": "#2C4251 25%",
            "footer-background": "#2C4251",
            "footer-key-foreground": "#F5F1E9",
            "footer-description-foreground": "#F5F1E9",
        },
    ),
    "hypernova": TextualTheme(
        name="hypernova",
        primary="#00F5D4",
        secondary="#7B2FF7",
        warning="#FEE440",
        error="#F72585",
        success="#80FF72",
        accent="#4CC9F0",
        background="#0B0B12",
        surface="#121225",
        panel="#1A1A32",
        dark=True,
        variables={
            "input-cursor-background": "#4CC9F0",
            "input-selection-background": "#4CC9F0 30%",
            "footer-background": "transparent",
        },
    ),
    "synthwave": TextualTheme(
        name="synthwave",
        primary="#FF006E",
        secondary="#8338EC",
        warning="#FFBE0B",
        error="#FB5607",
        success="#06FFA5",
        accent="#C77DFF",
        background="#0F0A19",
        surface="#1A0F26",
        panel="#251833",
        dark=True,
        variables={
            "input-cursor-background": "#FF006E",
            "input-selection-background": "#FF006E 25%",
            "footer-background": "transparent",
        },
    ),
    "amber": TextualTheme(
        name="amber",
        primary="#C15F3C",
        secondary="#B1ADA1",
        warning="#E8A550",
        error="#E05252",
        success="#5CB88A",
        accent="#C15F3C",
        background="#1A1612",
        surface="#252119",
        panel="#2F2A24",
        dark=True,
        variables={
            "input-cursor-background": "#C15F3C",
            "input-selection-background": "#C15F3C 30%",
            "block-cursor-background": "#3D2A1E",
            "block-cursor-foreground": "#F4F3EE",
            "block-cursor-text-style": "bold",
            "footer-background": "transparent",
            "footer-key-foreground": "#C15F3C",
            "footer-description-foreground": "#B1ADA1",
        },
    ),
}

# ── Register all base16 schemes ────────────────────────────────────────────────
from speek.speek_max.color_schemes import SCHEMES, base16_to_textual

for _name, _palette in SCHEMES.items():
    if _name not in SPEEK_THEMES:
        SPEEK_THEMES[_name] = base16_to_textual(_palette, _name)


# ── Load user YAML themes from ~/.config/speek-max/themes/ ─────────────────────

def _load_yaml_theme(path: Path) -> TextualTheme | None:
    """Load a single YAML theme file and convert to TextualTheme.

    Supported fields: name, primary, secondary, accent, background, surface, panel, etc.
    Minimal required fields: name, primary.
    """
    if not _HAS_YAML:
        return None
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except Exception as exc:
        log.warning('Failed to parse theme %s: %s', path, exc)
        return None

    name = data.get('name')
    if not name or not isinstance(name, str):
        log.warning('Theme file %s missing "name" field', path)
        return None
    if 'primary' not in data:
        log.warning('Theme file %s missing "primary" field', path)
        return None

    # Build TextualTheme kwargs
    kwargs: dict = {'name': name, 'dark': data.get('dark', True)}
    for key in _THEME_COLOR_KEYS:
        if key in data and data[key] is not None:
            kwargs[key] = str(data[key])

    # Collect variables: any key starting with "var-" or nested "variables" dict
    variables: dict[str, str] = {}
    if isinstance(data.get('variables'), dict):
        variables.update(data['variables'])
    for key, val in data.items():
        if key.startswith(_THEME_VAR_PREFIX) and val is not None:
            variables[key[len(_THEME_VAR_PREFIX):]] = str(val)
    if variables:
        kwargs['variables'] = variables

    try:
        return TextualTheme(**kwargs)
    except Exception as exc:
        log.warning('Invalid theme in %s: %s', path, exc)
        return None


def load_user_themes() -> dict[str, TextualTheme]:
    """Load all .yaml/.yml themes from ~/.config/speek-max/themes/.

    Returns:
        Dict mapping theme name to TextualTheme. Empty if dir doesn't
        exist or pyyaml is not installed.
    """
    themes: dict[str, TextualTheme] = {}
    if not _HAS_YAML or not _USER_THEME_DIR.is_dir():
        return themes
    for path in sorted(_USER_THEME_DIR.iterdir()):
        if path.suffix in ('.yaml', '.yml'):
            theme = _load_yaml_theme(path)
            if theme:
                themes[theme.name] = theme
    return themes


# Register user themes (override built-in if same name)
for _name, _theme in load_user_themes().items():
    SPEEK_THEMES[_name] = _theme


# ── Patch all themes: derive neutral greys for UI chrome ───────────────────────

def _to_neutral_grey(hex_color: str) -> str:
    """Convert a hex color to a neutral grey with the same perceived brightness."""
    h = hex_color.lstrip('#')
    if len(h) != 6:
        return hex_color
    r, g, b = int(h[:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    # Perceived luminance
    grey = int(0.299 * r + 0.587 * g + 0.114 * b)
    return f'#{grey:02x}{grey:02x}{grey:02x}'


for _theme in SPEEK_THEMES.values():
    v = _theme.variables
    _bg = _theme.background or '#1a1a1a'
    _sf = _theme.surface or '#2a2a2a'
    _pn = _theme.panel or '#333333'
    # Neutral greys matching the theme's brightness but with zero saturation
    _grey_bg = _to_neutral_grey(_bg)
    _grey_sf = _to_neutral_grey(_sf)
    _grey_pn = _to_neutral_grey(_pn)
    v.setdefault('border', _grey_pn)
    v.setdefault('border-blurred', _grey_sf)
    v.setdefault('scrollbar', _grey_pn)
    v.setdefault('scrollbar-background', _grey_bg)
    v.setdefault('scrollbar-active', _grey_pn)
    v.setdefault('scrollbar-hover', _grey_pn)
    # Neutral chrome colors for SCSS: table headers, dropdowns, etc.
    v.setdefault('chrome', _grey_sf)
    v.setdefault('chrome-active', _grey_pn)
    if v.get('footer-background') == 'transparent':
        v['footer-background'] = _bg

THEME_NAMES: list[str] = list(SPEEK_THEMES.keys())
DEFAULT_THEME: str = "galaxy"
