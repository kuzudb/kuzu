def test_version() -> None:
    import kuzu

    assert kuzu.version != ""
    assert kuzu.storage_version > 0
    assert kuzu.version == kuzu.__version__

    version_info = kuzu.storage_version_info
    for key, value in version_info.items():
        assert key != ""
        key_split = key.split(".")
        assert len(key_split) == 3 or len(key_split) == 4
        for part in key_split[:3]:
            assert part.isdigit()
            assert int(part) >= 0
        assert key_split[3].startswith("dev") if len(key_split) == 4 else True
        assert isinstance(value, int)
        assert value > 0
