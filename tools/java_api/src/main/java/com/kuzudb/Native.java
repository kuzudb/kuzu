package com.kuzudb;

import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Native is a wrapper class for the native library.
 * It is used to load the native library and call the native functions.
 * This class is not intended to be used by end users.
 */
public class Native {
    static {
        try {
            String os_name = "";
            String os_arch;
            String os_name_detect = System.getProperty("os.name").toLowerCase().trim();
            String os_arch_detect = System.getProperty("os.arch").toLowerCase().trim();
            boolean isAndroid = System.getProperty("java.runtime.name", "").toLowerCase().contains("android")
                || System.getProperty("java.vendor", "").toLowerCase().contains("android")
                || System.getProperty("java.vm.name", "").toLowerCase().contains("dalvik");
            switch (os_arch_detect) {
                case "x86_64":
                case "amd64":
                    os_arch = "amd64";
                    break;
                case "aarch64":
                case "arm64":
                    os_arch = "arm64";
                    break;
                case "i386":
                    os_arch = "i386";
                    break;
                default:
                    throw new IllegalStateException("Unsupported system architecture");
            }
            if (isAndroid){
                os_name = "android";
            }
            else if (os_name_detect.startsWith("windows")) {
                os_name = "windows";
            } else if (os_name_detect.startsWith("mac")) {
                os_name = "osx";
            } else if (os_name_detect.startsWith("linux")) {
                os_name = "linux";
            }
            String lib_res_name = "/libkuzu_java_native.so" + "_" + os_name + "_" + os_arch;

            Path lib_file = Files.createTempFile("libkuzu_java_native", ".so");
            URL lib_res = Native.class.getResource(lib_res_name);
            if (lib_res == null) {
                throw new IOException(lib_res_name + " not found");
            }
            Files.copy(lib_res.openStream(), lib_file, StandardCopyOption.REPLACE_EXISTING);
            new File(lib_file.toString()).deleteOnExit();
            String lib_path = lib_file.toAbsolutePath().toString();
            System.load(lib_path);
            if (os_name.equals("linux")) {
                kuzuNativeReloadLibrary(lib_path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Hack: Reload the native library again in JNI bindings to work around the
    // extension loading issue on Linux as System.load() does not set
    // `RTLD_GLOBAL` flag and there is no way to set it in Java.
    protected static native void kuzuNativeReloadLibrary(String libPath);

    // Database
    protected static native long kuzuDatabaseInit(String databasePath, long bufferPoolSize,
            boolean enableCompression, boolean readOnly, long maxDbSize, boolean autoCheckpoint,
            long checkpointThreshold,boolean throwOnWalReplayFailure, boolean enableChecksums);

    protected static native void kuzuDatabaseDestroy(Database db);

    protected static native void kuzuDatabaseSetLoggingLevel(String loggingLevel);

    // Connection
    protected static native long kuzuConnectionInit(Database database);

    protected static native void kuzuConnectionDestroy(Connection connection);

    protected static native void kuzuConnectionSetMaxNumThreadForExec(
            Connection connection, long numThreads);

    protected static native long kuzuConnectionGetMaxNumThreadForExec(Connection connection);

    protected static native QueryResult kuzuConnectionQuery(Connection connection, String query);

    protected static native PreparedStatement kuzuConnectionPrepare(
            Connection connection, String query);

    protected static native QueryResult kuzuConnectionExecute(
            Connection connection, PreparedStatement preparedStatement, Map<String, Value> param);

    protected static native void kuzuConnectionInterrupt(Connection connection);

    protected static native void kuzuConnectionSetQueryTimeout(
            Connection connection, long timeoutInMs);

    // PreparedStatement
    protected static native void kuzuPreparedStatementDestroy(PreparedStatement preparedStatement);

    protected static native boolean kuzuPreparedStatementIsSuccess(PreparedStatement preparedStatement);

    protected static native String kuzuPreparedStatementGetErrorMessage(
            PreparedStatement preparedStatement);

    // QueryResult
    protected static native void kuzuQueryResultDestroy(QueryResult queryResult);

    protected static native boolean kuzuQueryResultIsSuccess(QueryResult queryResult);

    protected static native String kuzuQueryResultGetErrorMessage(QueryResult queryResult);

    protected static native long kuzuQueryResultGetNumColumns(QueryResult queryResult);

    protected static native String kuzuQueryResultGetColumnName(QueryResult queryResult, long index);

    protected static native DataType kuzuQueryResultGetColumnDataType(
            QueryResult queryResult, long index);

    protected static native long kuzuQueryResultGetNumTuples(QueryResult queryResult);

    protected static native QuerySummary kuzuQueryResultGetQuerySummary(QueryResult queryResult);

    protected static native boolean kuzuQueryResultHasNext(QueryResult queryResult);

    protected static native FlatTuple kuzuQueryResultGetNext(QueryResult queryResult);

    protected static native boolean kuzuQueryResultHasNextQueryResult(QueryResult queryResult);

    protected static native QueryResult kuzuQueryResultGetNextQueryResult(QueryResult queryResult);

    protected static native String kuzuQueryResultToString(QueryResult queryResult);

    protected static native void kuzuQueryResultResetIterator(QueryResult queryResult);

    // FlatTuple
    protected static native void kuzuFlatTupleDestroy(FlatTuple flatTuple);

    protected static native Value kuzuFlatTupleGetValue(FlatTuple flatTuple, long index);

    protected static native String kuzuFlatTupleToString(FlatTuple flatTuple);

    // DataType
    protected static native long kuzuDataTypeCreate(
            DataTypeID id, DataType childType, long numElementsInArray);

    protected static native DataType kuzuDataTypeClone(DataType dataType);

    protected static native void kuzuDataTypeDestroy(DataType dataType);

    protected static native boolean kuzuDataTypeEquals(DataType dataType1, DataType dataType2);

    protected static native DataTypeID kuzuDataTypeGetId(DataType dataType);

    protected static native DataType kuzuDataTypeGetChildType(DataType dataType);

    protected static native long kuzuDataTypeGetNumElementsInArray(DataType dataType);

    // Value
    protected static native Value kuzuValueCreateNull();

    protected static native Value kuzuValueCreateNullWithDataType(DataType dataType);

    protected static native boolean kuzuValueIsNull(Value value);

    protected static native void kuzuValueSetNull(Value value, boolean isNull);

    protected static native Value kuzuValueCreateDefault(DataType dataType);

    protected static native <T> long kuzuValueCreateValue(T val);

    protected static native Value kuzuValueClone(Value value);

    protected static native void kuzuValueCopy(Value value, Value other);

    protected static native void kuzuValueDestroy(Value value);

    protected static native Value kuzuCreateMap(Value[] keys, Value[] values);

    protected static native Value kuzuCreateList(Value[] values);

    protected static native Value kuzuCreateList(DataType type, long numElements);

    protected static native long kuzuValueGetListSize(Value value);

    protected static native Value kuzuValueGetListElement(Value value, long index);

    protected static native DataType kuzuValueGetDataType(Value value);

    protected static native <T> T kuzuValueGetValue(Value value);

    protected static native String kuzuValueToString(Value value);

    protected static native InternalID kuzuNodeValGetId(Value nodeVal);

    protected static native String kuzuNodeValGetLabelName(Value nodeVal);

    protected static native long kuzuNodeValGetPropertySize(Value nodeVal);

    protected static native String kuzuNodeValGetPropertyNameAt(Value nodeVal, long index);

    protected static native Value kuzuNodeValGetPropertyValueAt(Value nodeVal, long index);

    protected static native String kuzuNodeValToString(Value nodeVal);

    protected static native InternalID kuzuRelValGetId(Value relVal);

    protected static native InternalID kuzuRelValGetSrcId(Value relVal);

    protected static native InternalID kuzuRelValGetDstId(Value relVal);

    protected static native String kuzuRelValGetLabelName(Value relVal);

    protected static native long kuzuRelValGetPropertySize(Value relVal);

    protected static native String kuzuRelValGetPropertyNameAt(Value relVal, long index);

    protected static native Value kuzuRelValGetPropertyValueAt(Value relVal, long index);

    protected static native String kuzuRelValToString(Value relVal);

    protected static native Value kuzuCreateStruct(String[] fieldNames, Value[] fieldValues);

    protected static native String kuzuValueGetStructFieldName(Value structVal, long index);

    protected static native long kuzuValueGetStructIndex(Value structVal, String fieldName);

    protected static native String kuzuGetVersion();

    protected static native long kuzuGetStorageVersion();
}
