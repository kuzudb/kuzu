from kuzu import __version__
from kuzu import __storage_version__

def test_version():
    assert __version__ != None
    assert __storage_version__ > 0