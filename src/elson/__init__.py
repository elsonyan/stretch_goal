from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("elson")
except PackageNotFoundError:
    __version__ = "(local)"

del PackageNotFoundError
del version