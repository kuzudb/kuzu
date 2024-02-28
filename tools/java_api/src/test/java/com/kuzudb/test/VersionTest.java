package com.kuzudb.java_test;

import com.kuzudb.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class VersionTest extends TestBase {

    @Test
    void GetVersion() {
        String version = KuzuVersion.getVersion();
        assertTrue(!version.equals(""));
    }

    @Test
    void GetStorageVersion() {
        long storage_version = KuzuVersion.getStorageVersion();
        assertTrue(storage_version > 0);
    }
}
