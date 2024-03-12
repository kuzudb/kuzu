def test_version() -> None:
    import kuzu

    assert kuzu.version != ""
    assert kuzu.storage_version > 0
