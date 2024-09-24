package com.kuzudb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class VersionTest extends TestBase {

    @Test
    void GetVersion() {
        String version = Version.getVersion();
        assertTrue(!version.equals(""));
    }

    @Test
    void GetStorageVersion() {
        long storage_version = Version.getStorageVersion();
        assertTrue(storage_version > 0);
    }
}
