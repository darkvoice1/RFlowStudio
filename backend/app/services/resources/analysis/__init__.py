"""Analysis resource service package.

Keep package initialization lightweight to avoid circular imports during
submodule loading. Import concrete services from their module files instead of
re-exporting them here.
"""
