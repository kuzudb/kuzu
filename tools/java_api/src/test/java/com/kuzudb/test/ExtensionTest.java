package com.kuzudb.java_test;

import com.kuzudb.*;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ExtensionTest extends TestBase {
    @Test
    void HttpfsInstallAndLoad() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("INSTALL httpfs");
        assertTrue(result.isSuccess());
        result.destroy();
        String currentDir = System.getProperty("user.dir");
        Path path = Path.of(currentDir, "../../extension/httpfs/build/libhttpfs.kuzu_extension");
        Path absPath = path.normalize().toAbsolutePath();
        String absPathStr = absPath.toString();
        absPathStr = absPathStr.replace("\\", "/");
        result = conn.query("LOAD EXTENSION " + "\"" + absPath + "\"");
        
        assertTrue(result.isSuccess());
        result.destroy();
    }
}
