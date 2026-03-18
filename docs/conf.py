"""Sphinx configuration for speek documentation."""

project = "speek"
copyright = "2024, Dongyeop Lee"
author = "Dongyeop Lee"
release = "0.0.3"

extensions = [
    "myst_parser",
    "autoapi.extension",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_design",
]

# MyST Markdown settings
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]
myst_heading_anchors = 3

# sphinx-autoapi settings
autoapi_type = "python"
autoapi_dirs = ["../speek"]
autoapi_root = "api"
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "imported-members",
]
autoapi_keep_files = False

# Napoleon settings for Google-style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True

# Theme
html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_title = "👁 speek"
html_theme_options = {
    "show_toc_level": 2,
    "repository_url": "https://github.com/dongyeoplee2/speek",
    "use_repository_button": True,
    "navigation_with_keys": False,
}
html_css_files = ["custom.css"]

suppress_warnings = [
    "autoapi.python_import_resolution",
    "myst.xref_missing",
]
nitpicky = False
