def test_version():
    import kuzu
    assert kuzu.version != ""
    assert kuzu.storage_version > 0
