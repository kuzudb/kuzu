package com.kuzudb.java_test;

import com.kuzudb.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ExtensionTest extends TestBase {
    @Test
    void HttpfsInstallAndLoad() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("INSTALL httpfs");
        assertTrue(result.isSuccess());
        result.destroy();
        result = conn.query("LOAD EXTENSION httpfs");
        assertTrue(result.isSuccess());
        result.destroy();
    }
}
