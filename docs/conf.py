"""Sphinx configuration. """

from datetime import datetime

project = "AsyncFlow"
copyright = f"{datetime.now().year}, Jacob Unna"
author = "Jacob Unna"
master_doc = "index"
release = "0.1"
extensions = ["sphinx.ext.autodoc"]
templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
html_theme = "alabaster"
html_static_path = ["_static"]
html_sidebars = {
    "**": ["about.html", "navigation.html",],
}
html_theme_options = {
    "description": "Concurrency made easy",
    "github_user": "jacobunna",
    "github_repo": "asyncflow",
}
