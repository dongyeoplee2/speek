"""color_schemes.py — Base16 color scheme definitions for speek-max.

# Base16 Styling Guidelines (v0.2)

speek-max uses the base16 standard for portable, interoperable color schemes.
This is the same format used by Neovim, VS Code, Alacritty, tmux, and hundreds
of other tools.

Reference:  https://github.com/chriskempson/base16/blob/main/styling.md
Vim ref:    https://github.com/chriskempson/base16-vim/
Schemes:    https://github.com/tinted-theming/schemes

## The 16 Slots

Colours base00 to base07 are variations of a shade, darkest to lightest.
Colours base08 to base0F are individual colours for types, operators, names.
For dark themes, base00–base07 span dark → light; for light themes, light → dark.

  base00  Default Background
  base01  Lighter Background (status bars, line numbers, folding marks)
  base02  Selection Background
  base03  Comments, Invisibles, Line Highlighting
  base04  Dark Foreground (status bars)
  base05  Default Foreground, Caret, Delimiters, Operators
  base06  Light Foreground (not often used)
  base07  Light Background (not often used)
  base08  Variables, XML Tags, Markup Link Text, Markup Lists, Diff Deleted
  base09  Integers, Boolean, Constants, XML Attributes, Markup Link Url
  base0A  Classes, Markup Bold, Search Text Background
  base0B  Strings, Inherited Class, Markup Code, Diff Inserted
  base0C  Support, Regular Expressions, Escape Characters, Markup Quotes
  base0D  Functions, Methods, Attribute IDs, Headings
  base0E  Keywords, Storage, Selector, Markup Italic, Diff Changed
  base0F  Deprecated, Opening/Closing Embedded Language Tags

## Mapping to Textual Theme Variables

  base00  → background         Default Background
  base01  → surface            Lighter Background (status bars)
  base02  → panel              Selection Background
  base03  → $border-color      Comments, Invisibles, Line Highlighting
  base04  → $muted-foreground  Dark Foreground (status bars)
  base05  → foreground         Default Foreground, Caret
  base08  → error              Variables, Diff Deleted (red)
  base09  → warning            Integers, Constants (orange)
  base0B  → success            Strings, Diff Inserted (green)
  base0C  → accent             Support, Regular Expressions (cyan)
  base0D  → primary            Functions, Methods, Headings (blue)
  base0E  → secondary          Keywords, Storage (purple)

## Adding New Schemes

Any base16-format scheme (a dict with keys "base00"…"base0F", each a 6-digit
hex string without the leading #) can be passed to `base16_to_textual()` to
produce a TextualTheme ready for use in speek-max.
"""
from __future__ import annotations

from textual.theme import Theme as TextualTheme

# ── Popular base16 schemes ────────────────────────────────────────────────────
# Palette values are 6-digit hex strings (no leading #).
# Sources: https://github.com/tinted-theming/schemes

SCHEMES: dict[str, dict[str, str]] = {
    # ── Official / well-known ──────────────────────────────────────────────
    # All values sourced from tinted-theming/schemes (spec-0.11) or the
    # individual upstream repos listed in the base16 scheme-list.

    "default-dark": {"base00":"181818","base01":"282828","base02":"383838","base03":"585858","base04":"b8b8b8","base05":"d8d8d8","base06":"e8e8e8","base07":"f8f8f8","base08":"ab4642","base09":"dc9656","base0A":"f7ca88","base0B":"a1b56c","base0C":"86c1b9","base0D":"7cafc2","base0E":"ba8baf","base0F":"a16946"},
    "tokyonight": {"base00":"1a1b26","base01":"16161e","base02":"2f3549","base03":"444b6a","base04":"787c99","base05":"a9b1d6","base06":"cbccd1","base07":"d5d6db","base08":"c0caf5","base09":"a9b1d6","base0A":"0db9d7","base0B":"9ece6a","base0C":"b4f9f8","base0D":"2ac3de","base0E":"bb9af7","base0F":"f7768e"},
    "catppuccin": {"base00":"1e1e2e","base01":"181825","base02":"313244","base03":"45475a","base04":"585b70","base05":"cdd6f4","base06":"f5e0dc","base07":"b4befe","base08":"f38ba8","base09":"fab387","base0A":"f9e2af","base0B":"a6e3a1","base0C":"94e2d5","base0D":"89b4fa","base0E":"cba6f7","base0F":"f2cdcd"},
    "gruvbox": {"base00":"282828","base01":"3c3836","base02":"504945","base03":"665c54","base04":"bdae93","base05":"d5c4a1","base06":"ebdbb2","base07":"fbf1c7","base08":"fb4934","base09":"fe8019","base0A":"fabd2f","base0B":"b8bb26","base0C":"8ec07c","base0D":"83a598","base0E":"d3869b","base0F":"d65d0e"},
    "nord": {"base00":"2e3440","base01":"3b4252","base02":"434c5e","base03":"4c566a","base04":"d8dee9","base05":"e5e9f0","base06":"eceff4","base07":"8fbcbb","base08":"bf616a","base09":"d08770","base0A":"ebcb8b","base0B":"a3be8c","base0C":"88c0d0","base0D":"81a1c1","base0E":"b48ead","base0F":"5e81ac"},
    "dracula": {"base00":"282a36","base01":"363447","base02":"44475a","base03":"6272a4","base04":"9ea8c7","base05":"f8f8f2","base06":"f0f1f4","base07":"ffffff","base08":"ff5555","base09":"ffb86c","base0A":"f1fa8c","base0B":"50fa7b","base0C":"8be9fd","base0D":"80bfff","base0E":"ff79c6","base0F":"bd93f9"},
    "rosepine": {"base00":"232136","base01":"2a273f","base02":"393552","base03":"6e6a86","base04":"908caa","base05":"e0def4","base06":"e0def4","base07":"56526e","base08":"eb6f92","base09":"f6c177","base0A":"ea9a97","base0B":"3e8fb0","base0C":"9ccfd8","base0D":"c4a7e7","base0E":"f6c177","base0F":"56526e"},
    "kanagawa": {"base00":"1f1f28","base01":"16161d","base02":"223249","base03":"54546d","base04":"727169","base05":"dcd7ba","base06":"c8c093","base07":"717c7c","base08":"c34043","base09":"ffa066","base0A":"c0a36e","base0B":"76946a","base0C":"6a9589","base0D":"7e9cd8","base0E":"957fb8","base0F":"d27e99"},
    "onedark": {"base00":"282c34","base01":"353b45","base02":"3e4451","base03":"545862","base04":"565c64","base05":"abb2bf","base06":"b6bdca","base07":"c8ccd4","base08":"e06c75","base09":"d19a66","base0A":"e5c07b","base0B":"98c379","base0C":"56b6c2","base0D":"61afef","base0E":"c678dd","base0F":"be5046"},
    "everforest": {"base00":"272e33","base01":"2e383c","base02":"414b50","base03":"859289","base04":"9da9a0","base05":"d3c6aa","base06":"edeada","base07":"fffbef","base08":"e67e80","base09":"e69875","base0A":"dbbc7f","base0B":"a7c080","base0C":"83c092","base0D":"7fbbb3","base0E":"d699b6","base0F":"9da9a0"},
    "solarized-dark": {"base00":"002b36","base01":"073642","base02":"586e75","base03":"657b83","base04":"839496","base05":"93a1a1","base06":"eee8d5","base07":"fdf6e3","base08":"dc322f","base09":"cb4b16","base0A":"b58900","base0B":"859900","base0C":"2aa198","base0D":"268bd2","base0E":"6c71c4","base0F":"d33682"},

    # ── Community contributed (alphabetical) ───────────────────────────────

    "apprentice": {"base00":"262626","base01":"AF5F5F","base02":"5F875F","base03":"87875F","base04":"5F87AF","base05":"5F5F87","base06":"5F8787","base07":"6C6C6C","base08":"444444","base09":"FF8700","base0A":"87AF87","base0B":"FFFFAF","base0C":"87AFD7","base0D":"8787AF","base0E":"5FAFAF","base0F":"BCBCBC"},
    "atelier-dune": {"base00":"20201d","base01":"292824","base02":"6e6b5e","base03":"7d7a68","base04":"999580","base05":"a6a28c","base06":"e8e4cf","base07":"fefbec","base08":"d73737","base09":"b65611","base0A":"ae9513","base0B":"60ac39","base0C":"1fad83","base0D":"6684e1","base0E":"b854d4","base0F":"d43552"},
    "atlas": {"base00":"002635","base01":"00384d","base02":"517F8D","base03":"6C8B91","base04":"869696","base05":"a1a19a","base06":"e6e6dc","base07":"fafaf8","base08":"ff5a67","base09":"f08e48","base0A":"ffcc1b","base0B":"7fc06e","base0C":"5dd7b9","base0D":"14747e","base0E":"9a70a4","base0F":"c43060"},
    "black-metal": {"base00":"000000","base01":"121212","base02":"222222","base03":"333333","base04":"999999","base05":"c1c1c1","base06":"999999","base07":"c1c1c1","base08":"5f8787","base09":"aaaaaa","base0A":"a06666","base0B":"dd9999","base0C":"aaaaaa","base0D":"888888","base0E":"999999","base0F":"444444"},
    "brogrammer": {"base00":"1f1f1f","base01":"f81118","base02":"2dc55e","base03":"ecba0f","base04":"2a84d2","base05":"4e5ab7","base06":"1081d6","base07":"d6dbe5","base08":"d6dbe5","base09":"de352e","base0A":"1dd361","base0B":"f3bd09","base0C":"1081d6","base0D":"5350b9","base0E":"0f7ddb","base0F":"ffffff"},
    "brushtrees-dark": {"base00":"485867","base01":"5A6D7A","base02":"6D828E","base03":"8299A1","base04":"98AFB5","base05":"B0C5C8","base06":"C9DBDC","base07":"E3EFEF","base08":"b38686","base09":"d8bba2","base0A":"aab386","base0B":"87b386","base0C":"86b3b3","base0D":"868cb3","base0E":"b386b2","base0F":"b39f9f"},
    "circus": {"base00":"191919","base01":"202020","base02":"303030","base03":"5f5a60","base04":"505050","base05":"a7a7a7","base06":"808080","base07":"ffffff","base08":"dc657d","base09":"4bb1a7","base0A":"c3ba63","base0B":"84b97c","base0C":"4bb1a7","base0D":"639ee4","base0E":"b888e2","base0F":"b888e2"},
    "classic-dark": {"base00":"151515","base01":"202020","base02":"303030","base03":"505050","base04":"B0B0B0","base05":"D0D0D0","base06":"E0E0E0","base07":"F5F5F5","base08":"AC4142","base09":"D28445","base0A":"F4BF75","base0B":"90A959","base0C":"75B5AA","base0D":"6A9FB5","base0E":"AA759F","base0F":"8F5536"},
    "codeschool": {"base00":"232c31","base01":"1c3657","base02":"2a343a","base03":"3f4944","base04":"84898c","base05":"9ea7a6","base06":"a7cfa3","base07":"b5d8f6","base08":"2a5491","base09":"43820d","base0A":"a03b1e","base0B":"237986","base0C":"b02f30","base0D":"484d79","base0E":"c59820","base0F":"c98344"},
    "colors": {"base00":"111111","base01":"333333","base02":"555555","base03":"777777","base04":"999999","base05":"bbbbbb","base06":"dddddd","base07":"ffffff","base08":"ff4136","base09":"ff851b","base0A":"ffdc00","base0B":"2ecc40","base0C":"7fdbff","base0D":"0074d9","base0E":"b10dc9","base0F":"85144b"},
    "cupertino": {"base00":"ffffff","base01":"c0c0c0","base02":"c0c0c0","base03":"808080","base04":"808080","base05":"404040","base06":"404040","base07":"5e5e5e","base08":"c41a15","base09":"eb8500","base0A":"826b28","base0B":"007400","base0C":"318495","base0D":"0000ff","base0E":"a90d91","base0F":"826b28"},
    "darcula": {"base00":"2b2b2b","base01":"323232","base02":"323232","base03":"606366","base04":"a4a3a3","base05":"a9b7c6","base06":"ffc66d","base07":"ffffff","base08":"4eade5","base09":"689757","base0A":"bbb529","base0B":"6a8759","base0C":"629755","base0D":"9876aa","base0E":"cc7832","base0F":"808080"},
    "danqing": {"base00":"2d302f","base01":"434846","base02":"5a605d","base03":"9da8a3","base04":"cad8d2","base05":"e0f0ef","base06":"ecf6f2","base07":"fcfefd","base08":"F9906F","base09":"B38A61","base0A":"F0C239","base0B":"8AB361","base0C":"30DFF3","base0D":"B0A4E3","base0E":"CCA4E3","base0F":"CA6924"},
    "darkmoss": {"base00":"171e1f","base01":"252c2d","base02":"373c3d","base03":"555e5f","base04":"818f80","base05":"c7c7a5","base06":"e3e3c8","base07":"e1eaef","base08":"ff4658","base09":"e6db74","base0A":"fdb11f","base0B":"499180","base0C":"66d9ef","base0D":"498091","base0E":"9bc0c8","base0F":"d27b53"},
    "darkviolet": {"base00":"000000","base01":"231a40","base02":"432d59","base03":"593380","base04":"00ff00","base05":"b08ae6","base06":"9045e6","base07":"a366ff","base08":"a82ee6","base09":"bb66cc","base0A":"f29df2","base0B":"4595e6","base0C":"40dfff","base0D":"4136d9","base0E":"7e5ce6","base0F":"a886bf"},
    "dirtysea": {"base00":"e0e0e0","base01":"d0dad0","base02":"d0d0d0","base03":"707070","base04":"202020","base05":"000000","base06":"f8f8f8","base07":"c4d9c4","base08":"840000","base09":"006565","base0A":"755B00","base0B":"730073","base0C":"755B00","base0D":"007300","base0E":"000090","base0F":"755B00"},
    "edge-dark": {"base00":"262729","base01":"313235","base02":"3d3f42","base03":"4a4c4f","base04":"95989d","base05":"afb2b5","base06":"caccce","base07":"e4e5e6","base08":"e77171","base09":"eba31a","base0A":"dbb774","base0B":"a1bf78","base0C":"5ebaa5","base0D":"73b3e7","base0E":"d390e7","base0F":"5ebaa5"},
    "equilibrium-dark": {"base00":"0c1118","base01":"181c22","base02":"22262d","base03":"7b776e","base04":"949088","base05":"afaba2","base06":"cac6bd","base07":"e7e2d9","base08":"f04339","base09":"df5923","base0A":"bb8801","base0B":"7f8b00","base0C":"00948b","base0D":"008dd1","base0E":"6a7fd2","base0F":"e3488e"},
    "espresso": {"base00":"2d2d2d","base01":"393939","base02":"515151","base03":"777777","base04":"b4b7b4","base05":"cccccc","base06":"e0e0e0","base07":"ffffff","base08":"d25252","base09":"f9a959","base0A":"ffc66d","base0B":"a5c261","base0C":"bed6ff","base0D":"6c99bb","base0E":"d197d9","base0F":"f97394"},
    "eva-dim": {"base00":"2a3b4d","base01":"3d566f","base02":"4b6988","base03":"55799c","base04":"7e90a3","base05":"9fa2a6","base06":"d6d7d9","base07":"ffffff","base08":"c4676c","base09":"ff9966","base0A":"cfd05d","base0B":"5de561","base0C":"4b8f77","base0D":"1ae1dc","base0E":"9c6cd3","base0F":"bb64a9"},
    "framer": {"base00":"181818","base01":"151515","base02":"464646","base03":"747474","base04":"B9B9B9","base05":"D0D0D0","base06":"E8E8E8","base07":"EEEEEE","base08":"FD886B","base09":"FC4769","base0A":"FECB6E","base0B":"32CCDC","base0C":"ACDDFD","base0D":"20BCFC","base0E":"BA8CFC","base0F":"B15F4A"},
    "fruit-soda": {"base00":"f1ecf1","base01":"e0dee0","base02":"d8d5d5","base03":"b5b4b6","base04":"979598","base05":"515151","base06":"474545","base07":"2d2c2c","base08":"fe3e31","base09":"fe6d08","base0A":"f7e203","base0B":"47f74c","base0C":"0f9cfd","base0D":"2931df","base0E":"611fce","base0F":"b16f40"},
    "gigavolt": {"base00":"202126","base01":"2d303d","base02":"5a576e","base03":"a1d2e6","base04":"cad3ff","base05":"e9e7e1","base06":"eff0f9","base07":"f2fbff","base08":"ff661a","base09":"19f988","base0A":"ffdc2d","base0B":"f2e6a9","base0C":"fb6acb","base0D":"40bfff","base0E":"ae94f9","base0F":"6187ff"},
    "github": {"base00":"eaeef2","base01":"d0d7de","base02":"afb8c1","base03":"8c959f","base04":"6e7781","base05":"424a53","base06":"32383f","base07":"1f2328","base08":"fa4549","base09":"e16f24","base0A":"bf8700","base0B":"2da44e","base0C":"339D9B","base0D":"218bff","base0E":"a475f9","base0F":"4d2d00"},
    "gruber": {"base00":"181818","base01":"453d41","base02":"484848","base03":"52494e","base04":"e4e4ef","base05":"f4f4ff","base06":"f5f5f5","base07":"e4e4ef","base08":"f43841","base09":"c73c3f","base0A":"ffdd33","base0B":"73c936","base0C":"95a99f","base0D":"96a6c8","base0E":"9e95c7","base0F":"cc8c3c"},
    "hardcore": {"base00":"212121","base01":"303030","base02":"353535","base03":"4A4A4A","base04":"707070","base05":"cdcdcd","base06":"e5e5e5","base07":"ffffff","base08":"f92672","base09":"fd971f","base0A":"e6db74","base0B":"a6e22e","base0C":"708387","base0D":"66d9ef","base0E":"9e6ffe","base0F":"e8b882"},
    "heetch": {"base00":"190134","base01":"392551","base02":"5A496E","base03":"7B6D8B","base04":"9C92A8","base05":"BDB6C5","base06":"DEDAE2","base07":"FEFFFF","base08":"27D9D5","base09":"5BA2B6","base0A":"8F6C97","base0B":"C33678","base0C":"F80059","base0D":"BD0152","base0E":"82034C","base0F":"470546"},
    "helios": {"base00":"1d2021","base01":"383c3e","base02":"53585b","base03":"6f7579","base04":"cdcdcd","base05":"d5d5d5","base06":"dddddd","base07":"e5e5e5","base08":"d72638","base09":"eb8413","base0A":"f19d1a","base0B":"88b92d","base0C":"1ba595","base0D":"1e8bac","base0E":"be4264","base0F":"c85e0d"},
    "horizon-dark": {"base00":"1C1E26","base01":"232530","base02":"2E303E","base03":"6F6F70","base04":"9DA0A2","base05":"CBCED0","base06":"DCDFE4","base07":"E3E6EE","base08":"E93C58","base09":"E58D7D","base0A":"EFB993","base0B":"EFAF8E","base0C":"24A8B4","base0D":"DF5273","base0E":"B072D1","base0F":"E4A382"},
    "humanoid-dark": {"base00":"232629","base01":"333b3d","base02":"484e54","base03":"60615d","base04":"c0c0bd","base05":"f8f8f2","base06":"fcfcf6","base07":"fcfcfc","base08":"f11235","base09":"ff9505","base0A":"ffb627","base0B":"02d849","base0C":"0dd9d6","base0D":"00a6fb","base0E":"f15ee3","base0F":"b27701"},
    "ia-dark": {"base00":"1a1a1a","base01":"222222","base02":"1d414d","base03":"767676","base04":"b8b8b8","base05":"cccccc","base06":"e8e8e8","base07":"f8f8f8","base08":"d88568","base09":"d86868","base0A":"b99353","base0B":"83a471","base0C":"7c9cae","base0D":"8eccdd","base0E":"b98eb2","base0F":"8b6c37"},
    "icy": {"base00":"021012","base01":"031619","base02":"041f23","base03":"052e34","base04":"064048","base05":"095b67","base06":"0c7c8c","base07":"109cb0","base08":"16c1d9","base09":"b3ebf2","base0A":"80deea","base0B":"4dd0e1","base0C":"26c6da","base0D":"00bcd4","base0E":"00acc1","base0F":"0097a7"},
    "kimber": {"base00":"222222","base01":"313131","base02":"555D55","base03":"644646","base04":"5A5A5A","base05":"DEDEE7","base06":"C3C3B4","base07":"FFFFE6","base08":"C88C8C","base09":"476C88","base0A":"D8B56D","base0B":"99C899","base0C":"78B4B4","base0D":"537C9C","base0E":"86CACD","base0F":"704F4F"},
    "materia": {"base00":"263238","base01":"2C393F","base02":"37474F","base03":"707880","base04":"C9CCD3","base05":"CDD3DE","base06":"D5DBE5","base07":"FFFFFF","base08":"EC5F67","base09":"EA9560","base0A":"FFCC00","base0B":"8BD649","base0C":"80CBC4","base0D":"89DDFF","base0E":"82AAFF","base0F":"EC5F67"},
    "material-darker": {"base00":"212121","base01":"303030","base02":"353535","base03":"4A4A4A","base04":"B2CCD6","base05":"EEFFFF","base06":"EEFFFF","base07":"FFFFFF","base08":"F07178","base09":"F78C6C","base0A":"FFCB6B","base0B":"C3E88D","base0C":"89DDFF","base0D":"82AAFF","base0E":"C792EA","base0F":"FF5370"},
    "material-vivid": {"base00":"202124","base01":"27292c","base02":"323639","base03":"44464d","base04":"676c71","base05":"80868b","base06":"9e9e9e","base07":"ffffff","base08":"f44336","base09":"ff9800","base0A":"ffeb3b","base0B":"00e676","base0C":"00bcd4","base0D":"2196f3","base0E":"673ab7","base0F":"8d6e63"},
    "mellow-purple": {"base00":"1e0528","base01":"1A092D","base02":"331354","base03":"320f55","base04":"873582","base05":"ffeeff","base06":"ffeeff","base07":"f8c0ff","base08":"00d9e9","base09":"aa00a3","base0A":"955ae7","base0B":"05cb0d","base0C":"b900b1","base0D":"550068","base0E":"8991bb","base0F":"4d6fff"},
    "mexico-light": {"base00":"f8f8f8","base01":"e8e8e8","base02":"d8d8d8","base03":"b8b8b8","base04":"585858","base05":"383838","base06":"282828","base07":"181818","base08":"ab4642","base09":"dc9656","base0A":"f79a0e","base0B":"538947","base0C":"4b8093","base0D":"7cafc2","base0E":"96609e","base0F":"a16946"},
    "nebula": {"base00":"22273b","base01":"414f60","base02":"5a8380","base03":"6e6f72","base04":"87888b","base05":"a4a6a9","base06":"c7c9cd","base07":"8dbdaa","base08":"777abc","base09":"94929e","base0A":"4f9062","base0B":"6562a8","base0C":"226f68","base0D":"4d6bb6","base0E":"716cae","base0F":"8c70a7"},
    "nova": {"base00":"3C4C55","base01":"556873","base02":"6A7D89","base03":"899BA6","base04":"899BA6","base05":"C5D4DD","base06":"899BA6","base07":"556873","base08":"83AFE5","base09":"7FC1CA","base0A":"A8CE93","base0B":"7FC1CA","base0C":"F2C38F","base0D":"83AFE5","base0E":"9A93E1","base0F":"F2C38F"},
    "one-light": {"base00":"fafafa","base01":"f0f0f1","base02":"e5e5e6","base03":"a0a1a7","base04":"696c77","base05":"383a42","base06":"202227","base07":"090a0b","base08":"ca1243","base09":"d75f00","base0A":"c18401","base0B":"50a14f","base0C":"0184bc","base0D":"4078f2","base0E":"a626a4","base0F":"986801"},
    "outrun-dark": {"base00":"00002A","base01":"20204A","base02":"30305A","base03":"50507A","base04":"B0B0DA","base05":"D0D0FA","base06":"E0E0FF","base07":"F5F5FF","base08":"FF4242","base09":"FC8D28","base0A":"F3E877","base0B":"59F176","base0C":"0EF0F0","base0D":"66B0FF","base0E":"F10596","base0F":"F003EF"},
    "papercolor-dark": {"base00":"1c1c1c","base01":"363636","base02":"424242","base03":"585858","base04":"808080","base05":"9e9e9e","base06":"b8b8b8","base07":"d0d0d0","base08":"ff5faf","base09":"d7af5f","base0A":"ffaf00","base0B":"5faf5f","base0C":"00afaf","base0D":"5fafd7","base0E":"af87d7","base0F":"af005f"},
    "pasque": {"base00":"271C3A","base01":"100323","base02":"3E2D5C","base03":"5D5766","base04":"BEBCBF","base05":"DEDCDF","base06":"EDEAEF","base07":"BBAADD","base08":"A92258","base09":"918889","base0A":"804ead","base0B":"C6914B","base0C":"7263AA","base0D":"8E7DC6","base0E":"953B9D","base0F":"59325C"},
    "pinky": {"base00":"171517","base01":"1b181b","base02":"1d1b1d","base03":"383338","base04":"e7dbdb","base05":"f5f5f5","base06":"ffffff","base07":"f7f3f7","base08":"ffa600","base09":"00ff66","base0A":"20df6c","base0B":"ff0066","base0C":"6600ff","base0D":"00ffff","base0E":"007fff","base0F":"df206c"},
    "porple": {"base00":"292c36","base01":"333344","base02":"474160","base03":"65568a","base04":"b8b8b8","base05":"d8d8d8","base06":"e8e8e8","base07":"f8f8f8","base08":"f84547","base09":"d28e5d","base0A":"efa16b","base0B":"95c76f","base0C":"64878f","base0D":"8485ce","base0E":"b74989","base0F":"986841"},
    "purpledream": {"base00":"100510","base01":"302030","base02":"403040","base03":"605060","base04":"bbb0bb","base05":"ddd0dd","base06":"eee0ee","base07":"fff0ff","base08":"FF1D0D","base09":"CCAE14","base0A":"F000A0","base0B":"14CC64","base0C":"0075B0","base0D":"00A0F0","base0E":"B000D0","base0F":"6A2A3C"},
    "qualia": {"base00":"101010","base01":"454545","base02":"454545","base03":"454545","base04":"808080","base05":"C0C0C0","base06":"C0C0C0","base07":"454545","base08":"EFA6A2","base09":"A3B8EF","base0A":"E6A3DC","base0B":"80C990","base0C":"C8C874","base0D":"50CACD","base0E":"E0AF85","base0F":"808080"},
    "rebecca": {"base00":"292a44","base01":"663399","base02":"383a62","base03":"666699","base04":"a0a0c5","base05":"f1eff8","base06":"ccccff","base07":"53495d","base08":"a0a0c5","base09":"efe4a1","base0A":"ae81ff","base0B":"6dfedf","base0C":"8eaee0","base0D":"2de0a7","base0E":"7aa5ff","base0F":"ff79c6"},
    "sagelight": {"base00":"f8f8f8","base01":"e8e8e8","base02":"d8d8d8","base03":"b8b8b8","base04":"585858","base05":"383838","base06":"282828","base07":"181818","base08":"fa8480","base09":"ffaa61","base0A":"ffdc61","base0B":"a0d2c8","base0C":"a2d6f5","base0D":"a0a7d2","base0E":"c8a0d2","base0F":"d2b2a0"},
    "sakura": {"base00":"feedf3","base01":"f8e2e7","base02":"e0ccd1","base03":"755f64","base04":"665055","base05":"564448","base06":"42383a","base07":"33292b","base08":"df2d52","base09":"f6661e","base0A":"c29461","base0B":"2e916d","base0C":"1d8991","base0D":"006e93","base0E":"5e2180","base0F":"ba0d35"},
    "sandcastle": {"base00":"282c34","base01":"2c323b","base02":"3e4451","base03":"665c54","base04":"928374","base05":"a89984","base06":"d5c4a1","base07":"fdf4c1","base08":"83a598","base09":"a07e3b","base0A":"a07e3b","base0B":"528b8b","base0C":"83a598","base0D":"83a598","base0E":"d75f5f","base0F":"a87322"},
    "shades-of-purple": {"base00":"1e1e3f","base01":"43d426","base02":"f1d000","base03":"808080","base04":"6871ff","base05":"c7c7c7","base06":"ff77ff","base07":"ffffff","base08":"d90429","base09":"f92a1c","base0A":"ffe700","base0B":"3ad900","base0C":"00c5c7","base0D":"6943ff","base0E":"ff2c70","base0F":"79e8fb"},
    "silk-dark": {"base00":"0e3c46","base01":"1D494E","base02":"2A5054","base03":"587073","base04":"9DC8CD","base05":"C7DBDD","base06":"CBF2F7","base07":"D2FAFF","base08":"fb6953","base09":"fcab74","base0A":"fce380","base0B":"73d8ad","base0C":"3fb2b9","base0D":"46bddd","base0E":"756b8a","base0F":"9b647b"},
    "snazzy": {"base00":"282a36","base01":"34353e","base02":"43454f","base03":"78787e","base04":"a5a5a9","base05":"e2e4e5","base06":"eff0eb","base07":"f1f1f0","base08":"ff5c57","base09":"ff9f43","base0A":"f3f99d","base0B":"5af78e","base0C":"9aedfe","base0D":"57c7ff","base0E":"ff6ac1","base0F":"b2643c"},
    "solarflare": {"base00":"18262F","base01":"222E38","base02":"586875","base03":"667581","base04":"85939E","base05":"A6AFB8","base06":"E8E9ED","base07":"F5F7FA","base08":"EF5253","base09":"E66B2B","base0A":"E4B51C","base0B":"7CC844","base0C":"52CBB0","base0D":"33B5E1","base0E":"A363D5","base0F":"D73C9A"},
    "summercamp": {"base00":"1c1810","base01":"2a261c","base02":"3a3527","base03":"504b38","base04":"5f5b45","base05":"736e55","base06":"bab696","base07":"f8f5de","base08":"e35142","base09":"fba11b","base0A":"f2ff27","base0B":"5ceb5a","base0C":"5aebbc","base0D":"489bf0","base0E":"FF8080","base0F":"F69BE7"},
    "summerfruit-dark": {"base00":"151515","base01":"202020","base02":"303030","base03":"505050","base04":"B0B0B0","base05":"D0D0D0","base06":"E0E0E0","base07":"FFFFFF","base08":"FF0086","base09":"FD8900","base0A":"ABA800","base0B":"00C918","base0C":"1FAAAA","base0D":"3777E6","base0E":"AD00A1","base0F":"CC6633"},
    "synth-midnight": {"base00":"050608","base01":"1a1b1c","base02":"28292a","base03":"474849","base04":"a3a5a6","base05":"c1c3c4","base06":"cfd1d2","base07":"dddfe0","base08":"b53b50","base09":"ea770d","base0A":"c9d364","base0B":"06ea61","base0C":"42fff9","base0D":"03aeff","base0E":"ea5ce2","base0F":"cd6320"},
    "tango": {"base00":"2e3436","base01":"8ae234","base02":"fce94f","base03":"555753","base04":"729fcf","base05":"d3d7cf","base06":"ad7fa8","base07":"eeeeec","base08":"cc0000","base09":"ef2929","base0A":"c4a000","base0B":"4e9a06","base0C":"06989a","base0D":"3465a4","base0E":"75507b","base0F":"34e2e2"},
    "tender": {"base00":"282828","base01":"383838","base02":"484848","base03":"4c4c4c","base04":"b8b8b8","base05":"eeeeee","base06":"e8e8e8","base07":"feffff","base08":"f43753","base09":"dc9656","base0A":"ffc24b","base0B":"c9d05c","base0C":"73cef4","base0D":"b3deef","base0E":"d3b987","base0F":"a16946"},
    "twilight": {"base00":"1e1e1e","base01":"323537","base02":"464b50","base03":"5f5a60","base04":"838184","base05":"a7a7a7","base06":"c3c3c3","base07":"ffffff","base08":"cf6a4c","base09":"cda869","base0A":"f9ee98","base0B":"8f9d6a","base0C":"afc4db","base0D":"7587a6","base0E":"9b859d","base0F":"9b703f"},
    "unikitty-dark": {"base00":"2e2a31","base01":"4a464d","base02":"666369","base03":"838085","base04":"9f9da2","base05":"bcbabe","base06":"d8d7da","base07":"f5f4f7","base08":"d8137f","base09":"d65407","base0A":"dc8a0e","base0B":"17ad98","base0C":"149bda","base0D":"796af5","base0E":"bb60ea","base0F":"c720ca"},
    "vice": {"base00":"17191E","base01":"22262d","base02":"3c3f4c","base03":"383a47","base04":"555e70","base05":"8b9cbe","base06":"b2bfd9","base07":"f4f4f7","base08":"ff29a8","base09":"85ffe0","base0A":"f0ffaa","base0B":"0badff","base0C":"8265ff","base0D":"00eaff","base0E":"00f6d9","base0F":"ff3d81"},
    "vulcan": {"base00":"041523","base01":"122339","base02":"003552","base03":"7a5759","base04":"6b6977","base05":"5b778c","base06":"333238","base07":"214d68","base08":"818591","base09":"9198a3","base0A":"adb4b9","base0B":"977d7c","base0C":"977d7c","base0D":"977d7c","base0E":"9198a3","base0F":"977d7c"},
    "windows-nt": {"base00":"000000","base01":"2a2a2a","base02":"555555","base03":"808080","base04":"a1a1a1","base05":"c0c0c0","base06":"e0e0e0","base07":"ffffff","base08":"ff0000","base09":"808000","base0A":"ffff00","base0B":"00ff00","base0C":"00ffff","base0D":"0000ff","base0E":"ff00ff","base0F":"008000"},
    "woodland": {"base00":"231e18","base01":"302b25","base02":"48413a","base03":"9d8b70","base04":"b4a490","base05":"cabcb1","base06":"d7c8bc","base07":"e4d4c8","base08":"d35c5c","base09":"ca7f32","base0A":"e0ac16","base0B":"b7ba53","base0C":"6eb958","base0D":"88a4d3","base0E":"bb90e2","base0F":"b49368"},
    "xcode-dusk": {"base00":"282B35","base01":"3D4048","base02":"53555D","base03":"686A71","base04":"7E8086","base05":"939599","base06":"A9AAAE","base07":"BEBFC2","base08":"B21889","base09":"786DC5","base0A":"438288","base0B":"DF0002","base0C":"00A0BE","base0D":"790EAD","base0E":"B21889","base0F":"C77C48"},
    "zenburn": {"base00":"383838","base01":"404040","base02":"606060","base03":"6f6f6f","base04":"808080","base05":"dcdccc","base06":"c0c0c0","base07":"ffffff","base08":"dca3a3","base09":"dfaf8f","base0A":"e0cf9f","base0B":"5f7f5f","base0C":"93e0e3","base0D":"7cb8bb","base0E":"dc8cc3","base0F":"000000"},
}


def _is_dark(scheme: dict[str, str]) -> bool:
    """Return True if the scheme is dark (base00 luminance < 0.5)."""
    bg = scheme.get('base00', '000000')
    r, g, b = int(bg[:2], 16), int(bg[2:4], 16), int(bg[4:6], 16)
    lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return lum < 0.5


# ── Converter ─────────────────────────────────────────────────────────────────

def base16_to_textual(scheme: dict[str, str], name: str) -> TextualTheme:
    """Convert a base16 palette dict to a Textual TextualTheme.

    Args:
        scheme: Dict with keys "base00"–"base0F", values are 6-digit hex strings
                (without leading #).
        name:   Theme name string (must be unique within SPEEK_THEMES).

    Returns:
        A TextualTheme instance ready to register with a Textual app.

    Base16 slot → Textual mapping:
        base00 → background        Default Background
        base01 → surface           Lighter Background (status bars, line numbers)
        base02 → panel             Selection Background
        base03 → (muted text)      Comments, Invisibles, Line Highlighting
        base04 → (dark fg)         Dark Foreground (status bars)
        base05 → foreground        Default Foreground, Caret, Delimiters
        base08 → error             Variables, XML Tags, Diff Deleted (red)
        base09 → warning           Integers, Boolean, Constants (orange)
        base0B → success           Strings, Diff Inserted (green)
        base0C → accent            Support, Regular Expressions (cyan)
        base0D → primary           Functions, Methods, Headings (blue)
        base0E → secondary         Keywords, Storage, Selector (purple)
    """
    def h(key: str) -> str:
        return f'#{scheme[key]}'

    return TextualTheme(
        name=name,
        primary=h('base0D'),       # Functions, Methods (blue)
        secondary=h('base0E'),     # Keywords, Storage (purple)
        accent=h('base0C'),        # Support, RegExps (cyan)
        warning=h('base09'),       # Integers, Constants (orange)
        error=h('base08'),         # Variables, Diff Deleted (red)
        success=h('base0B'),       # Strings, Diff Inserted (green)
        background=h('base00'),    # Default Background
        surface=h('base01'),       # Lighter Background (status bars)
        panel=h('base02'),         # Selection Background
        dark=_is_dark(scheme),
        variables={
            'block-cursor-foreground': h('base05'),   # Default Foreground
            'block-cursor-background': h('base02'),   # Selection Background
            'block-cursor-text-style': 'bold',
            'input-cursor-background': h('base0D'),   # Functions (blue)
            'input-selection-background': h('base02'), # Selection Background
            'footer-background': h('base00'),          # Match default background
            'border-color': h('base03'),               # Comments, Line Highlighting
            'muted-foreground': h('base04'),           # Dark Foreground (status bars)
            # Override Textual's auto-derived blue borders/scrollbars
            'border': h('base03'),                     # Neutral: Comments color
            'border-blurred': h('base02'),             # Neutral: Selection bg
            'scrollbar': h('base03'),                  # Neutral scrollbar thumb
            'scrollbar-background': h('base01'),       # Surface for scrollbar track
            'scrollbar-active': h('base04'),           # Slightly brighter on drag
            'scrollbar-hover': h('base04'),            # Slightly brighter on hover
        },
    )
