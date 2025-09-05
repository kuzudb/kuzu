#include <unordered_map>

#ifdef _WIN32
// Do nothing on Windows
#else
#include <dlfcn.h>
#endif

// This header is generated at build time. See CMakeLists.txt.
#include <vector>

#include "com_kuzudb_Native.h"
#include "common/constants.h"
#include "common/exception/exception.h"
#include "common/exception/not_implemented.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"
#include "main/kuzu.h"
#include <jni.h>

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;

#ifdef __ANDROID__
static jint JNI_VERSION = JNI_VERSION_1_6;
#else
static jint JNI_VERSION = JNI_VERSION_1_8;
#endif

// map
static jclass J_C_Map;
static jmethodID J_C_Map_M_entrySet;
// set
static jclass J_C_Set;
static jmethodID J_C_Set_M_iterator;
// iterator
static jclass J_C_Iterator;
static jmethodID J_C_Iterator_M_hasNext;
static jmethodID J_C_Iterator_M_next;
// Map$Entry
static jclass J_C_Map$Entry;
static jmethodID J_C_Map$Entry_M_getKey;
static jmethodID J_C_Map$Entry_M_getValue;
// Exception
static jclass J_C_Exception;
// QueryResult
static jclass J_C_QueryResult;
static jfieldID J_C_QueryResult_F_qr_ref;
static jfieldID J_C_QueryResult_F_isOwnedByCPP;
// PreparedStatement
static jclass J_C_PreparedStatement;
static jfieldID J_C_PreparedStatement_F_ps_ref;
// DataType
static jclass J_C_DataType;
static jfieldID J_C_DataType_F_dt_ref;
// QuerySummary
static jclass J_C_QuerySummary;
static jmethodID J_C_QuerySummary_M_ctor;
// FlatTuple
static jclass J_C_FlatTuple;
static jfieldID J_C_FlatTuple_F_ft_ref;
// Value
static jclass J_C_Value;
static jfieldID J_C_Value_F_v_ref;
static jfieldID J_C_Value_F_isOwnedByCPP;
// DataTypeID
static jclass J_C_DataTypeID;
static jfieldID J_C_DataTypeID_F_value;
// Boolean
static jclass J_C_Boolean;
static jmethodID J_C_Boolean_M_init;
static jmethodID J_C_Boolean_M_booleanValue;
// Long
static jclass J_C_Long;
static jmethodID J_C_Long_M_init;
static jmethodID J_C_Long_M_longValue;
// Integer
static jclass J_C_Integer;
static jmethodID J_C_Integer_M_init;
static jmethodID J_C_Integer_M_intValue;
// InternalID
static jclass J_C_InternalID;
static jmethodID J_C_InternalID_M_init;
static jfieldID J_C_InternalID_F_tableId;
static jfieldID J_C_InternalID_F_offset;
// Double
static jclass J_C_Double;
static jmethodID J_C_Double_M_init;
static jmethodID J_C_Double_M_doubleValue;
// BigDecimal
static jclass J_C_BigDecimal;
static jmethodID J_C_BigDecimal_M_init;
static jmethodID J_C_BigDecimal_M_toString;
static jmethodID J_C_BigDecimal_M_precision;
static jmethodID J_C_BigDecimal_M_scale;
// LocalDate
static jclass J_C_LocalDate;
static jmethodID J_C_LocalDate_M_ofEpochDay;
static jmethodID J_C_LocalDate_M_toEpochDay;
static jmethodID J_C_LocalDate_M_getEpochSecond;
static jmethodID J_C_LocalDate_M_getNano;

// Instant
static jclass J_C_Instant;
static jmethodID J_C_Instant_M_ofEpochSecond;
// Short
static jclass J_C_Short;
static jmethodID J_C_Short_M_init;
static jmethodID J_C_Short_M_shortValue;
// Byte
static jclass J_C_Byte;
static jmethodID J_C_Byte_M_init;
static jmethodID J_C_Byte_M_byteValue;
// BigInteger
static jclass J_C_BigInteger;
static jmethodID J_C_BigInteger_M_init;
static jmethodID J_C_BigInteger_M_longValue;
static jmethodID J_C_BigInteger_M_shiftRight;
// Float
static jclass J_C_Float;
static jmethodID J_C_Float_M_init;
static jmethodID J_C_Float_M_floatValue;
// Duration
static jclass J_C_Duration;
static jmethodID J_C_Duration_M_ofMillis;
static jmethodID J_C_Duration_M_toMillis;
// UUID
static jclass J_C_UUID;
static jmethodID J_C_UUID_M_init;
static jmethodID J_C_UUID_M_getMostSignificantBits;
static jmethodID J_C_UUID_M_getLeastSignificantBits;
// Connection
static jclass J_C_Connection;
static jfieldID J_C_Connection_F_conn_ref;
// Database
static jclass J_C_Database;
static jfieldID J_C_Database_db_ref;
// String
static jclass J_C_String;
static jmethodID J_C_String_M_ctor;
static jmethodID J_C_String_M_getBytes;

static void throwJNIException(JNIEnv* env, const char* message) {
    jclass exClass = env->FindClass("java/lang/RuntimeException");
    if (exClass == nullptr) {
        return;
    }
    env->ThrowNew(exClass, message);
}

static std::string jstringToUtf8String(JNIEnv* env, jstring value) {
    jbyteArray byteArr =
        (jbyteArray)env->CallObjectMethod(value, J_C_String_M_getBytes, env->NewStringUTF("UTF-8"));
    size_t length = env->GetArrayLength(byteArr);
    jbyte* bytes = env->GetByteArrayElements(byteArr, nullptr);
    std::string ret = std::string((char*)bytes, length);
    env->ReleaseByteArrayElements(byteArr, bytes, 0);
    return ret;
}

static jstring utf8StringToJstring(JNIEnv* env, std::string str) {
    jbyteArray byteArr = env->NewByteArray(str.size());
    env->SetByteArrayRegion(byteArr, 0, str.size(), reinterpret_cast<const jbyte*>(str.data()));
    jstring ret =
        (jstring)env->NewObject(J_C_String, J_C_String_M_ctor, byteArr, env->NewStringUTF("UTF-8"));
    return ret;
}

jobject createJavaObject(JNIEnv* env, void* memAddress, jclass javaClass, jfieldID refID) {
    try {
        auto address = reinterpret_cast<uint64_t>(memAddress);
        auto ref = static_cast<jlong>(address);

        jobject newObject = env->AllocObject(javaClass);

        env->SetLongField(newObject, refID, ref);
        return newObject;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

Database* getDatabase(JNIEnv* env, jobject thisDB) {
    try {
        jlong fieldValue = env->GetLongField(thisDB, J_C_Database_db_ref);

        uint64_t address = static_cast<uint64_t>(fieldValue);
        Database* db = reinterpret_cast<Database*>(address);
        return db;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return nullptr;
}

Connection* getConnection(JNIEnv* env, jobject thisConn) {

    try {
        jlong fieldValue = env->GetLongField(thisConn, J_C_Connection_F_conn_ref);

        uint64_t address = static_cast<uint64_t>(fieldValue);
        Connection* conn = reinterpret_cast<Connection*>(address);
        return conn;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return nullptr;
}

PreparedStatement* getPreparedStatement(JNIEnv* env, jobject thisPS) {
    try {
        jlong fieldValue = env->GetLongField(thisPS, J_C_PreparedStatement_F_ps_ref);

        uint64_t address = static_cast<uint64_t>(fieldValue);
        PreparedStatement* ps = reinterpret_cast<PreparedStatement*>(address);
        return ps;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return nullptr;
}

QueryResult* getQueryResult(JNIEnv* env, jobject thisQR) {
    try {
        jlong fieldValue = env->GetLongField(thisQR, J_C_QueryResult_F_qr_ref);

        uint64_t address = static_cast<uint64_t>(fieldValue);
        QueryResult* qr = reinterpret_cast<QueryResult*>(address);
        return qr;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return nullptr;
}

FlatTuple* getFlatTuple(JNIEnv* env, jobject thisFT) {
    try {
        jlong fieldValue = env->GetLongField(thisFT, J_C_FlatTuple_F_ft_ref);

        uint64_t address = static_cast<uint64_t>(fieldValue);
        auto ft = reinterpret_cast<std::shared_ptr<FlatTuple>*>(address);
        return ft->get();
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return nullptr;
}

LogicalType* getDataType(JNIEnv* env, jobject thisDT) {
    try {
        jlong fieldValue = env->GetLongField(thisDT, J_C_DataType_F_dt_ref);

        uint64_t address = static_cast<uint64_t>(fieldValue);
        auto* dt = reinterpret_cast<LogicalType*>(address);
        return dt;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return nullptr;
}

Value* getValue(JNIEnv* env, jobject thisValue) {
    try {
        jlong fieldValue = env->GetLongField(thisValue, J_C_Value_F_v_ref);

        uint64_t address = static_cast<uint64_t>(fieldValue);
        Value* v = reinterpret_cast<Value*>(address);
        return v;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return nullptr;
}

internalID_t getInternalID(JNIEnv* env, jobject id) {
    try {
        table_id_t table_id =
            static_cast<table_id_t>(env->GetLongField(id, J_C_InternalID_F_tableId));
        offset_t offset = static_cast<offset_t>(env->GetLongField(id, J_C_InternalID_F_offset));
        return internalID_t(offset, table_id);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return internalID_t();
}

std::string dataTypeToString(const LogicalType& dataType) {
    return LogicalTypeUtils::toString(dataType.getLogicalTypeID());
}

std::unordered_map<std::string, std::unique_ptr<Value>> javaMapToCPPMap(JNIEnv* env,
    jobject javaMap) {
    try {
        jobject set = env->CallObjectMethod(javaMap, J_C_Map_M_entrySet);
        jobject iter = env->CallObjectMethod(set, J_C_Set_M_iterator);

        std::unordered_map<std::string, std::unique_ptr<Value>> result;
        while (env->CallBooleanMethod(iter, J_C_Iterator_M_hasNext)) {
            jobject entry = env->CallObjectMethod(iter, J_C_Iterator_M_next);
            jstring key = (jstring)env->CallObjectMethod(entry, J_C_Map$Entry_M_getKey);
            jobject value = env->CallObjectMethod(entry, J_C_Map$Entry_M_getValue);
            std::string keyStr = jstringToUtf8String(env, key);
            const Value* v = getValue(env, value);
            // Java code can keep a reference to the value, so we cannot move.
            result.insert({keyStr, v->copy()});

            env->DeleteLocalRef(entry);
            env->DeleteLocalRef(key);
            env->DeleteLocalRef(value);
        }
        return result;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return std::unordered_map<std::string, std::unique_ptr<Value>>();
}

/**
 * All Database native functions
 */
//     protected static native void kuzuNativeReloadLibrary(String libPath);
JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuNativeReloadLibrary(JNIEnv* env, jclass,
    jstring libPath) {
    try {
#ifdef _WIN32
// Do nothing on Windows
#else
        const char* path = env->GetStringUTFChars(libPath, JNI_FALSE);
        void* handle = dlopen(path, RTLD_LAZY | RTLD_GLOBAL);
        env->ReleaseStringUTFChars(libPath, path);
        if (handle == nullptr) {
            auto error = dlerror(); // NOLINT(concurrency-mt-unsafe): load can only be executed in
                                    // single thread.
            env->ThrowNew(J_C_Exception, error);
        }
#endif
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuDatabaseInit(JNIEnv* env, jclass,
    jstring databasePath, jlong bufferPoolSize, jboolean enableCompression, jboolean readOnly,
    jlong maxDbSize, jboolean autoCheckpoint, jlong checkpointThreshold,
    jboolean throwOnWalReplayFailure, jboolean enableChecksums) {
    try {
        const char* path = env->GetStringUTFChars(databasePath, JNI_FALSE);
        uint64_t buffer = static_cast<uint64_t>(bufferPoolSize);
        SystemConfig systemConfig{};
        systemConfig.bufferPoolSize = buffer == 0 ? systemConfig.bufferPoolSize : buffer;
        systemConfig.enableCompression = enableCompression;
        systemConfig.readOnly = readOnly;
        systemConfig.maxDBSize = maxDbSize == 0 ? systemConfig.maxDBSize : maxDbSize;
        systemConfig.autoCheckpoint = autoCheckpoint;
        systemConfig.checkpointThreshold =
            checkpointThreshold >= 0 ? checkpointThreshold : systemConfig.checkpointThreshold;
        systemConfig.throwOnWalReplayFailure = throwOnWalReplayFailure;
        systemConfig.enableChecksums = enableChecksums;
        try {
            Database* db = new Database(path, systemConfig);
            uint64_t address = reinterpret_cast<uint64_t>(db);

            env->ReleaseStringUTFChars(databasePath, path);
            return static_cast<jlong>(address);
        } catch (Exception& e) {
            env->ReleaseStringUTFChars(databasePath, path);
            env->ThrowNew(J_C_Exception, e.what());
        }
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return 0;
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuDatabaseDestroy(JNIEnv* env, jclass,
    jobject thisDB) {
    try {
        Database* db = getDatabase(env, thisDB);
        delete db;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

/**
 * All Connection native functions
 */

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuConnectionInit(JNIEnv* env, jclass, jobject db) {

    try {
        Database* conn_db = getDatabase(env, db);

        Connection* conn = new Connection(conn_db);
        uint64_t connAddress = reinterpret_cast<uint64_t>(conn);

        return static_cast<jlong>(connAddress);
    } catch (Exception& e) {
        throwJNIException(env, e.what());
    }
    return 0;
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuConnectionDestroy(JNIEnv* env, jclass,
    jobject thisConn) {
    try {
        Connection* conn = getConnection(env, thisConn);
        delete conn;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuConnectionSetMaxNumThreadForExec(JNIEnv* env,
    jclass, jobject thisConn, jlong numThreads) {
    try {
        Connection* conn = getConnection(env, thisConn);
        uint64_t threads = static_cast<uint64_t>(numThreads);
        conn->setMaxNumThreadForExec(threads);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuConnectionGetMaxNumThreadForExec(JNIEnv* env,
    jclass, jobject thisConn) {
    try {
        Connection* conn = getConnection(env, thisConn);
        uint64_t threads = conn->getMaxNumThreadForExec();
        jlong numThreads = static_cast<jlong>(threads);
        return numThreads;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return 0;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuConnectionQuery(JNIEnv* env, jclass,
    jobject thisConn, jstring query) {
    try {
        Connection* conn = getConnection(env, thisConn);
        std::string cppQuery = jstringToUtf8String(env, query);
        auto queryResult = conn->query(cppQuery).release();

        uint64_t qrAddress = reinterpret_cast<uint64_t>(queryResult);
        jlong qrRef = static_cast<jlong>(qrAddress);

        jobject newQRObject = env->AllocObject(J_C_QueryResult);
        env->SetLongField(newQRObject, J_C_QueryResult_F_qr_ref, qrRef);
        return newQRObject;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuConnectionPrepare(JNIEnv* env, jclass,
    jobject thisConn, jstring query) {
    try {
        Connection* conn = getConnection(env, thisConn);
        std::string cppQuery = jstringToUtf8String(env, query);

        PreparedStatement* preparedStatement = conn->prepare(cppQuery).release();
        if (preparedStatement == nullptr) {
            return nullptr;
        }

        jobject ret = createJavaObject(env, preparedStatement, J_C_PreparedStatement,
            J_C_PreparedStatement_F_ps_ref);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuConnectionExecute(JNIEnv* env, jclass,
    jobject thisConn, jobject preStm, jobject paramMap) {
    try {
        Connection* conn = getConnection(env, thisConn);
        PreparedStatement* ps = getPreparedStatement(env, preStm);

        std::unordered_map<std::string, std::unique_ptr<Value>> params =
            javaMapToCPPMap(env, paramMap);

        auto queryResult = conn->executeWithParams(ps, std::move(params)).release();
        if (queryResult == nullptr) {
            return nullptr;
        }

        jobject ret = createJavaObject(env, queryResult, J_C_QueryResult, J_C_QueryResult_F_qr_ref);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuConnectionInterrupt(JNIEnv* env, jclass,
    jobject thisConn) {
    try {
        Connection* conn = getConnection(env, thisConn);
        conn->interrupt();
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuConnectionSetQueryTimeout(JNIEnv* env, jclass,
    jobject thisConn, jlong timeoutInMs) {
    try {
        Connection* conn = getConnection(env, thisConn);
        uint64_t timeout = static_cast<uint64_t>(timeoutInMs);
        conn->setQueryTimeOut(timeout);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

/**
 * All PreparedStatement native functions
 */

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuPreparedStatementDestroy(JNIEnv* env, jclass,
    jobject thisPS) {
    try {
        PreparedStatement* ps = getPreparedStatement(env, thisPS);
        delete ps;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_Native_kuzuPreparedStatementIsSuccess(JNIEnv* env,
    jclass, jobject thisPS) {
    try {
        PreparedStatement* ps = getPreparedStatement(env, thisPS);
        return static_cast<jboolean>(ps->isSuccess());
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jboolean();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuPreparedStatementGetErrorMessage(JNIEnv* env,
    jclass, jobject thisPS) {
    try {
        PreparedStatement* ps = getPreparedStatement(env, thisPS);
        std::string errorMessage = ps->getErrorMessage();
        jstring msg = utf8StringToJstring(env, errorMessage);
        return msg;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

/**
 * All QueryResult native functions
 */

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuQueryResultDestroy(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        delete qr;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_Native_kuzuQueryResultIsSuccess(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        return static_cast<jboolean>(qr->isSuccess());
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jboolean();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuQueryResultGetErrorMessage(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        std::string errorMessage = qr->getErrorMessage();
        jstring msg = utf8StringToJstring(env, errorMessage);
        return msg;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuQueryResultGetNumColumns(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        return static_cast<jlong>(qr->getNumColumns());
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuQueryResultGetColumnName(JNIEnv* env, jclass,
    jobject thisQR, jlong index) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        auto columnNames = qr->getColumnNames();
        uint64_t idx = static_cast<uint64_t>(index);
        if (idx >= columnNames.size()) {
            return nullptr;
        }
        std::string columnName = columnNames[idx];
        jstring name = utf8StringToJstring(env, columnName);
        return name;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuQueryResultGetColumnDataType(JNIEnv* env,
    jclass, jobject thisQR, jlong index) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        auto columnDataTypes = qr->getColumnDataTypes();
        uint64_t idx = static_cast<uint64_t>(index);
        if (idx >= columnDataTypes.size()) {
            return nullptr;
        }
        auto columnDataType = columnDataTypes[idx].copy();
        auto* cdtCopy = new LogicalType(std::move(columnDataType));

        uint64_t dtAddress = reinterpret_cast<uint64_t>(cdtCopy);
        jlong dtRef = static_cast<jlong>(dtAddress);

        jobject newDTObject = env->AllocObject(J_C_DataType);
        env->SetLongField(newDTObject, J_C_DataType_F_dt_ref, dtRef);
        return newDTObject;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuQueryResultGetNumTuples(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        return static_cast<jlong>(qr->getNumTuples());
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuQueryResultGetQuerySummary(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        auto querySummary = qr->getQuerySummary();

        jdouble cmpTime = static_cast<jdouble>(querySummary->getCompilingTime());
        jdouble exeTime = static_cast<jdouble>(querySummary->getExecutionTime());

        jobject newQSObject =
            env->NewObject(J_C_QuerySummary, J_C_QuerySummary_M_ctor, cmpTime, exeTime);
        return newQSObject;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_Native_kuzuQueryResultHasNext(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        return static_cast<jboolean>(qr->hasNext());
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jboolean();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuQueryResultGetNext(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        auto flatTuple = qr->getNext();

        auto newFT = new std::shared_ptr<FlatTuple>(flatTuple);
        uint64_t ftAddress = reinterpret_cast<uint64_t>(newFT);
        jlong ftRef = static_cast<jlong>(ftAddress);

        jobject newFTObject = env->AllocObject(J_C_FlatTuple);
        env->SetLongField(newFTObject, J_C_FlatTuple_F_ft_ref, ftRef);
        return newFTObject;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_Native_kuzuQueryResultHasNextQueryResult(JNIEnv* env,
    jclass, jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        return qr->hasNextQueryResult();
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jboolean();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuQueryResultGetNextQueryResult(JNIEnv* env,
    jclass, jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        auto queryResult = qr->getNextQueryResult();
        if (queryResult == nullptr) {
            return nullptr;
        }

        jobject ret = createJavaObject(env, queryResult, J_C_QueryResult, J_C_QueryResult_F_qr_ref);
        env->SetBooleanField(ret, J_C_QueryResult_F_isOwnedByCPP, static_cast<jboolean>(true));
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuQueryResultToString(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        std::string resultString = qr->toString();
        jstring ret = utf8StringToJstring(env, resultString);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuQueryResultResetIterator(JNIEnv* env, jclass,
    jobject thisQR) {
    try {
        QueryResult* qr = getQueryResult(env, thisQR);
        qr->resetIterator();
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

/**
 * All FlatTuple native functions
 */

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuFlatTupleDestroy(JNIEnv* env, jclass,
    jobject thisFT) {
    try {
        jlong fieldValue = env->GetLongField(thisFT, J_C_FlatTuple_F_ft_ref);

        uint64_t address = static_cast<uint64_t>(fieldValue);
        auto flat_tuple_shared_ptr = reinterpret_cast<std::shared_ptr<FlatTuple>*>(address);

        flat_tuple_shared_ptr->reset();
        delete flat_tuple_shared_ptr;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuFlatTupleGetValue(JNIEnv* env, jclass,
    jobject thisFT, jlong index) {
    try {
        FlatTuple* ft = getFlatTuple(env, thisFT);
        Value* value = nullptr;
        try {
            value = ft->getValue(index);
        } catch (Exception& e) {
            return nullptr;
        }

        jobject v = createJavaObject(env, value, J_C_Value, J_C_Value_F_v_ref);
        env->SetBooleanField(v, J_C_Value_F_isOwnedByCPP, static_cast<jboolean>(true));

        return v;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuFlatTupleToString(JNIEnv* env, jclass,
    jobject thisFT) {
    try {
        FlatTuple* ft = getFlatTuple(env, thisFT);
        std::string resultString = ft->toString();
        jstring ret = utf8StringToJstring(env, resultString);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

/**
 * All DataType native functions
 */

namespace kuzu::common {
struct JavaAPIHelper {
    static inline LogicalType* createLogicalType(LogicalTypeID typeID,
        std::unique_ptr<ExtraTypeInfo> extraTypeInfo) {
        return new LogicalType(typeID, std::move(extraTypeInfo));
    }
};
} // namespace kuzu::common

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuDataTypeCreate(JNIEnv* env, jclass, jobject id,
    jobject childType, jlong numElementsInArray) {
    try {
        jint fieldValue = env->GetIntField(id, J_C_DataTypeID_F_value);

        uint8_t dataTypeIdU8 = static_cast<uint8_t>(fieldValue);
        LogicalType* dataType = nullptr;
        auto logicalTypeID = static_cast<LogicalTypeID>(dataTypeIdU8);
        if (childType == nullptr) {
            dataType = new LogicalType(logicalTypeID);
        } else {
            auto childTypePty = getDataType(env, childType)->copy();
            auto extraTypeInfo =
                numElementsInArray > 0 ?
                    std::make_unique<ArrayTypeInfo>(std::move(childTypePty), numElementsInArray) :
                    std::make_unique<ListTypeInfo>(std::move(childTypePty));
            dataType = JavaAPIHelper::createLogicalType(logicalTypeID, std::move(extraTypeInfo));
        }
        uint64_t address = reinterpret_cast<uint64_t>(dataType);
        return static_cast<jlong>(address);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuDataTypeClone(JNIEnv* env, jclass,
    jobject thisDT) {
    try {
        auto* oldDT = getDataType(env, thisDT);
        auto* newDT = new LogicalType(oldDT->copy());

        jobject dt = createJavaObject(env, newDT, J_C_DataType, J_C_DataType_F_dt_ref);
        return dt;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuDataTypeDestroy(JNIEnv* env, jclass,
    jobject thisDT) {
    try {
        auto* dt = getDataType(env, thisDT);
        delete dt;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_Native_kuzuDataTypeEquals(JNIEnv* env, jclass,
    jobject dt1, jobject dt2) {
    try {
        auto* cppdt1 = getDataType(env, dt1);
        auto* cppdt2 = getDataType(env, dt2);

        return static_cast<jboolean>(*cppdt1 == *cppdt2);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jboolean();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuDataTypeGetId(JNIEnv* env, jclass,
    jobject thisDT) {
    try {
        auto* dt = getDataType(env, thisDT);
        std::string idStr = dataTypeToString(*dt);
        jfieldID idField =
            env->GetStaticFieldID(J_C_DataTypeID, idStr.c_str(), "Lcom/kuzudb/DataTypeID;");
        jobject id = env->GetStaticObjectField(J_C_DataTypeID, idField);
        return id;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuDataTypeGetChildType(JNIEnv* env, jclass,
    jobject thisDT) {
    try {
        auto* parentType = getDataType(env, thisDT);
        LogicalType childType;
        if (parentType->getLogicalTypeID() == LogicalTypeID::ARRAY) {
            childType = ArrayType::getChildType(*parentType).copy();
        } else if (parentType->getLogicalTypeID() == LogicalTypeID::LIST) {
            childType = ListType::getChildType(*parentType).copy();
        } else {
            return nullptr;
        }
        auto* newChildType = new LogicalType(std::move(childType));
        jobject ret = createJavaObject(env, newChildType, J_C_DataType, J_C_DataType_F_dt_ref);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuDataTypeGetNumElementsInArray(JNIEnv* env,
    jclass, jobject thisDT) {
    try {
        auto* dt = getDataType(env, thisDT);
        if (dt->getLogicalTypeID() != LogicalTypeID::ARRAY) {
            return 0;
        }
        return static_cast<jlong>(ArrayType::getNumElements(*dt));
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

/**
 * All Value native functions
 */

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuValueCreateNull(JNIEnv* env, jclass) {
    try {
        Value* v = new Value(Value::createNullValue());
        jobject ret = createJavaObject(env, v, J_C_Value, J_C_Value_F_v_ref);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuValueCreateNullWithDataType(JNIEnv* env,
    jclass, jobject dataType) {
    try {
        auto* dt = getDataType(env, dataType);
        Value* v = new Value(Value::createNullValue(*dt));
        jobject ret = createJavaObject(env, v, J_C_Value, J_C_Value_F_v_ref);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_Native_kuzuValueIsNull(JNIEnv* env, jclass,
    jobject thisV) {
    try {
        Value* v = getValue(env, thisV);
        return static_cast<jboolean>(v->isNull());
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jboolean();
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuValueSetNull(JNIEnv* env, jclass, jobject thisV,
    jboolean isNull) {
    try {
        Value* v = getValue(env, thisV);
        v->setNull(static_cast<bool>(isNull));
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuValueCreateDefault(JNIEnv* env, jclass,
    jobject dataType) {
    try {
        auto* dt = getDataType(env, dataType);
        Value* v = new Value(Value::createDefaultValue(*dt));
        jobject ret = createJavaObject(env, v, J_C_Value, J_C_Value_F_v_ref);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuValueCreateValue(JNIEnv* env, jclass,
    jobject val) {
    try {
        Value* v = nullptr;
        if (env->IsInstanceOf(val, J_C_Boolean)) {
            jboolean value = env->CallBooleanMethod(val, J_C_Boolean_M_booleanValue);
            v = new Value(static_cast<bool>(value));
        } else if (env->IsInstanceOf(val, J_C_Byte)) {
            jbyte value = env->CallByteMethod(val, J_C_Byte_M_byteValue);
            v = new Value(static_cast<int8_t>(value));
        } else if (env->IsInstanceOf(val, J_C_Short)) {
            jshort value = env->CallShortMethod(val, J_C_Short_M_shortValue);
            v = new Value(static_cast<int16_t>(value));
        } else if (env->IsInstanceOf(val, J_C_Integer)) {
            jint value = env->CallIntMethod(val, J_C_Integer_M_intValue);
            v = new Value(static_cast<int32_t>(value));
        } else if (env->IsInstanceOf(val, J_C_Long)) {
            jlong value = env->CallLongMethod(val, J_C_Long_M_longValue);
            v = new Value(static_cast<int64_t>(value));
        } else if (env->IsInstanceOf(val, J_C_BigInteger)) {
            int64_t lower =
                static_cast<int64_t>(env->CallLongMethod(val, J_C_BigInteger_M_longValue));
            jobject shifted = env->CallObjectMethod(val, J_C_BigInteger_M_shiftRight, 64);
            int64_t upper =
                static_cast<int64_t>(env->CallLongMethod(shifted, J_C_BigInteger_M_longValue));
            v = new Value(int128_t(lower, upper));
        } else if (env->IsInstanceOf(val, J_C_Float)) {
            jfloat value = env->CallFloatMethod(val, J_C_Float_M_floatValue);
            v = new Value(static_cast<float>(value));
        } else if (env->IsInstanceOf(val, J_C_Double)) {
            jdouble value = env->CallDoubleMethod(val, J_C_Double_M_doubleValue);
            v = new Value(static_cast<double>(value));
        } else if (env->IsInstanceOf(val, J_C_BigDecimal)) {
            jstring value =
                static_cast<jstring>(env->CallObjectMethod(val, J_C_BigDecimal_M_toString));
            std::string str = jstringToUtf8String(env, value);
            auto precision =
                static_cast<int32_t>(env->CallIntMethod(val, J_C_BigDecimal_M_precision));
            auto scale = static_cast<int32_t>(env->CallIntMethod(val, J_C_BigDecimal_M_scale));
            if (precision > DECIMAL_PRECISION_LIMIT) {
                throw NotImplementedException(
                    stringFormat("Decimal precision cannot be greater than {}"
                                 "Note: positive exponents contribute to precision",
                        DECIMAL_PRECISION_LIMIT));
            }
            auto type = LogicalType::DECIMAL(precision, scale);
            auto tmp = Value::createDefaultValue(type);
            int128_t res = 0;
            kuzu::function::decimalCast(str.c_str(), str.length(), res, type);
            tmp.val.int128Val = res;
            v = new Value(tmp);
        } else if (env->IsInstanceOf(val, J_C_String)) {
            jstring value = static_cast<jstring>(val);
            std::string str = jstringToUtf8String(env, value);
            v = new Value(str.c_str());
        } else if (env->IsInstanceOf(val, J_C_InternalID)) {
            int64_t tableId =
                static_cast<int64_t>(env->GetLongField(val, J_C_InternalID_F_tableId));
            int64_t offset = static_cast<int64_t>(env->GetLongField(val, J_C_InternalID_F_offset));
            internalID_t id(offset, tableId);
            v = new Value(id);
        } else if (env->IsInstanceOf(val, J_C_UUID)) {
            int64_t upper =
                static_cast<int64_t>(env->CallLongMethod(val, J_C_UUID_M_getMostSignificantBits));
            uint64_t lower =
                static_cast<uint64_t>(env->CallLongMethod(val, J_C_UUID_M_getLeastSignificantBits));
            int128_t uuid(lower, upper ^ (int64_t(1) << 63));
            v = new Value(ku_uuid_t{uuid});
        } else if (env->IsInstanceOf(val, J_C_LocalDate)) {
            int64_t days =
                static_cast<int64_t>(env->CallLongMethod(val, J_C_LocalDate_M_toEpochDay));
            v = new Value(date_t(days));
        } else if (env->IsInstanceOf(val, J_C_Instant)) {
            // TODO: Need to review this for overflow
            int64_t seconds =
                static_cast<int64_t>(env->CallLongMethod(val, J_C_LocalDate_M_getEpochSecond));
            int64_t nano = static_cast<int64_t>(env->CallLongMethod(val, J_C_LocalDate_M_getNano));

            int64_t micro = (seconds * 1000000L) + (nano / 1000L);
            v = new Value(timestamp_t(micro));
        } else if (env->IsInstanceOf(val, J_C_Duration)) {
            auto milis = env->CallLongMethod(val, J_C_Duration_M_toMillis);
            v = new Value(interval_t(0, 0, milis * 1000L));
        } else {
            throwJNIException(env, "Type of value is not supported in value_create_value");
            return -1;
        }
        uint64_t address = reinterpret_cast<uint64_t>(v);
        return static_cast<jlong>(address);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuValueClone(JNIEnv* env, jclass,
    jobject thisValue) {
    try {
        Value* v = getValue(env, thisValue);
        Value* copy = new Value(*v);
        return createJavaObject(env, copy, J_C_Value, J_C_Value_F_v_ref);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuValueCopy(JNIEnv* env, jclass, jobject thisValue,
    jobject otherValue) {
    try {
        Value* thisV = getValue(env, thisValue);
        Value* otherV = getValue(env, otherValue);
        thisV->copyValueFrom(*otherV);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT void JNICALL Java_com_kuzudb_Native_kuzuValueDestroy(JNIEnv* env, jclass,
    jobject thisValue) {
    try {
        Value* v = getValue(env, thisValue);
        delete v;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuCreateList___3Lcom_kuzudb_Value_2(JNIEnv* env,
    jclass, jobjectArray listValues) {
    try {
        jsize len = env->GetArrayLength(listValues);
        if (len == 0) {
            return nullptr;
        }

        std::vector<std::unique_ptr<Value>> children;
        for (jsize i = 0; i < len; ++i) {
            Value* element = getValue(env, env->GetObjectArrayElement(listValues, i));
            children.emplace_back(element->copy());
        }
        LogicalType childType = children[0]->getDataType().copy();

        Value* listValue = new Value(LogicalType::LIST(std::move(childType)), std::move(children));
        return createJavaObject(env, listValue, J_C_Value, J_C_Value_F_v_ref);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuCreateList__Lcom_kuzudb_DataType_2J(
    JNIEnv* env, jclass, jobject dataType, jlong numElements) {
    try {
        LogicalType* logicalType = getDataType(env, dataType);

        std::vector<std::unique_ptr<Value>> children;
        for (jlong i = 0; i < numElements; ++i) {
            children.emplace_back(std::make_unique<Value>(Value::createDefaultValue(*logicalType)));
        }

        Value* listValue = new Value(LogicalType::LIST(logicalType->copy()), std::move(children));
        return createJavaObject(env, listValue, J_C_Value, J_C_Value_F_v_ref);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuValueGetListSize(JNIEnv* env, jclass,
    jobject thisValue) {
    try {
        Value* v = getValue(env, thisValue);
        return static_cast<jlong>(NestedVal::getChildrenSize(v));
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuValueGetListElement(JNIEnv* env, jclass,
    jobject thisValue, jlong index) {
    try {
        Value* v = getValue(env, thisValue);
        uint64_t idx = static_cast<uint64_t>(index);

        auto size = NestedVal::getChildrenSize(v);
        if (idx >= size) {
            return nullptr;
        }

        auto val = NestedVal::getChildVal(v, idx);

        jobject element = createJavaObject(env, val, J_C_Value, J_C_Value_F_v_ref);
        env->SetBooleanField(element, J_C_Value_F_isOwnedByCPP, static_cast<jboolean>(true));
        return element;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuValueGetDataType(JNIEnv* env, jclass,
    jobject thisValue) {
    try {
        Value* v = getValue(env, thisValue);
        auto* dt = new LogicalType(v->getDataType().copy());
        return createJavaObject(env, dt, J_C_DataType, J_C_DataType_F_dt_ref);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuValueGetValue(JNIEnv* env, jclass,
    jobject thisValue) {
    try {
        Value* v = getValue(env, thisValue);
        const auto& dt = v->getDataType();
        auto logicalTypeId = dt.getLogicalTypeID();

        switch (logicalTypeId) {
        case LogicalTypeID::BOOL: {
            jboolean val = static_cast<jboolean>(v->getValue<bool>());
            jobject ret = env->NewObject(J_C_Boolean, J_C_Boolean_M_init, val);
            return ret;
        }
        case LogicalTypeID::INT64:
        case LogicalTypeID::SERIAL: {
            jlong val = static_cast<jlong>(v->getValue<int64_t>());
            jobject ret = env->NewObject(J_C_Long, J_C_Long_M_init, val);
            return ret;
        }
        case LogicalTypeID::INT32: {
            jint val = static_cast<jint>(v->getValue<int32_t>());
            jobject ret = env->NewObject(J_C_Integer, J_C_Integer_M_init, val);
            return ret;
        }
        case LogicalTypeID::INT16: {
            jshort val = static_cast<jshort>(v->getValue<int16_t>());
            jobject ret = env->NewObject(J_C_Short, J_C_Short_M_init, val);
            return ret;
        }
        case LogicalTypeID::INT8: {
            jbyte val = static_cast<jbyte>(v->getValue<int8_t>());
            jobject ret = env->NewObject(J_C_Byte, J_C_Byte_M_init, val);
            return ret;
        }
        case LogicalTypeID::UINT64: {
            std::string value = v->toString();
            jstring val = env->NewStringUTF(value.c_str());
            jobject ret = env->NewObject(J_C_BigInteger, J_C_BigInteger_M_init, val);
            return ret;
        }
        case LogicalTypeID::UINT32: {
            jlong val = static_cast<jlong>(v->getValue<uint32_t>());
            jobject ret = env->NewObject(J_C_Long, J_C_Long_M_init, val);
            return ret;
        }
        case LogicalTypeID::UINT16: {
            jint val = static_cast<jint>(v->getValue<uint16_t>());
            jobject ret = env->NewObject(J_C_Integer, J_C_Integer_M_init, val);
            return ret;
        }
        case LogicalTypeID::UINT8: {
            jshort val = static_cast<jshort>(v->getValue<uint8_t>());
            jobject ret = env->NewObject(J_C_Short, J_C_Short_M_init, val);
            return ret;
        }
        case LogicalTypeID::INT128: {
            int128_t int128_val = v->getValue<int128_t>();
            jstring val = env->NewStringUTF(Int128_t::ToString(int128_val).c_str());
            jobject ret = env->NewObject(J_C_BigInteger, J_C_BigInteger_M_init, val);
            return ret;
        }
        case LogicalTypeID::DOUBLE: {
            jdouble val = static_cast<jdouble>(v->getValue<double>());
            jobject ret = env->NewObject(J_C_Double, J_C_Double_M_init, val);
            return ret;
        }
        case LogicalTypeID::DECIMAL: {
            jstring val = env->NewStringUTF(v->toString().c_str());
            jobject ret = env->NewObject(J_C_BigDecimal, J_C_BigDecimal_M_init, val);
            return ret;
        }
        case LogicalTypeID::FLOAT: {
            jfloat val = static_cast<jfloat>(v->getValue<float>());
            jobject ret = env->NewObject(J_C_Float, J_C_Float_M_init, val);
            return ret;
        }
        case LogicalTypeID::DATE: {
            date_t date = v->getValue<date_t>();
            jobject ret = env->CallStaticObjectMethod(J_C_LocalDate, J_C_LocalDate_M_ofEpochDay,
                static_cast<jlong>(date.days));
            return ret;
        }
        case LogicalTypeID::TIMESTAMP_TZ: {
            timestamp_tz_t ts = v->getValue<timestamp_tz_t>();
            int64_t seconds = ts.value / 1000000L;
            int64_t nano = ts.value % 1000000L * 1000L;
            jobject ret = env->CallStaticObjectMethod(J_C_Instant, J_C_Instant_M_ofEpochSecond,
                seconds, nano);
            return ret;
        }
        case LogicalTypeID::TIMESTAMP: {
            timestamp_t ts = v->getValue<timestamp_t>();
            int64_t seconds = ts.value / 1000000L;
            int64_t nano = ts.value % 1000000L * 1000L;
            jobject ret = env->CallStaticObjectMethod(J_C_Instant, J_C_Instant_M_ofEpochSecond,
                seconds, nano);
            return ret;
        }
        case LogicalTypeID::TIMESTAMP_NS: {
            timestamp_ns_t ts = v->getValue<timestamp_ns_t>();
            int64_t seconds = ts.value / 1000000000L;
            int64_t nano = ts.value % 1000000000L;
            jobject ret = env->CallStaticObjectMethod(J_C_Instant, J_C_Instant_M_ofEpochSecond,
                seconds, nano);
            return ret;
        }
        case LogicalTypeID::TIMESTAMP_MS: {
            timestamp_ms_t ts = v->getValue<timestamp_ms_t>();
            int64_t seconds = ts.value / 1000L;
            int64_t nano = ts.value % 1000L * 1000000L;
            jobject ret = env->CallStaticObjectMethod(J_C_Instant, J_C_Instant_M_ofEpochSecond,
                seconds, nano);
            return ret;
        }
        case LogicalTypeID::TIMESTAMP_SEC: {
            timestamp_sec_t ts = v->getValue<timestamp_sec_t>();
            jobject ret =
                env->CallStaticObjectMethod(J_C_Instant, J_C_Instant_M_ofEpochSecond, ts.value, 0);
            return ret;
        }
        case LogicalTypeID::INTERVAL: {
            interval_t in = v->getValue<interval_t>();
            int64_t millis = Interval::getMicro(in) / 1000;
            jobject ret =
                env->CallStaticObjectMethod(J_C_Duration, J_C_Duration_M_ofMillis, millis);
            return ret;
        }
        case LogicalTypeID::INTERNAL_ID: {
            internalID_t iid = v->getValue<internalID_t>();
            jobject ret =
                env->NewObject(J_C_InternalID, J_C_InternalID_M_init, iid.tableID, iid.offset);
            return ret;
        }
        case LogicalTypeID::UUID: {
            int128_t uuid = v->getValue<int128_t>();
            jlong high = static_cast<jlong>(static_cast<uint64_t>(uuid.high ^ (int64_t(1) << 63)));
            jlong low = static_cast<jlong>(static_cast<uint64_t>(uuid.low));
            jobject ret = env->NewObject(J_C_UUID, J_C_UUID_M_init, high, low);
            return ret;
        }
        case LogicalTypeID::STRING: {
            std::string str = v->getValue<std::string>();
            jstring ret = utf8StringToJstring(env, str);
            return ret;
        }
        case LogicalTypeID::BLOB: {
            auto str = v->getValue<std::string>();
            auto byteBuffer = str.c_str();
            auto ret = env->NewByteArray(str.size());
            env->SetByteArrayRegion(ret, 0, str.size(), (jbyte*)byteBuffer);
            return ret;
        }
        default:
            throwJNIException(env, "Type of value is not supported in value_get_value");
            return nullptr;
        }
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return nullptr;
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuValueToString(JNIEnv* env, jclass,
    jobject thisValue) {
    try {
        Value* v = getValue(env, thisValue);
        std::string result_string = v->toString();
        jstring ret = utf8StringToJstring(env, result_string);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuNodeValGetId(JNIEnv* env, jclass,
    jobject thisNV) {
    try {
        auto nv = getValue(env, thisNV);
        auto idVal = NodeVal::getNodeIDVal(nv);
        if (idVal == nullptr) {
            return NULL;
        }
        auto id = idVal->getValue<internalID_t>();
        jobject ret = env->NewObject(J_C_InternalID, J_C_InternalID_M_init, id.tableID, id.offset);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuNodeValGetLabelName(JNIEnv* env, jclass,
    jobject thisNV) {
    try {
        auto* nv = getValue(env, thisNV);
        auto labelVal = NodeVal::getLabelVal(nv);
        if (labelVal == nullptr) {
            return NULL;
        }
        std::string label = labelVal->getValue<std::string>();
        return utf8StringToJstring(env, label);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuNodeValGetPropertySize(JNIEnv* env, jclass,
    jobject thisNV) {
    try {
        auto* nv = getValue(env, thisNV);
        auto size = NodeVal::getNumProperties(nv);
        return static_cast<jlong>(size);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuNodeValGetPropertyNameAt(JNIEnv* env, jclass,
    jobject thisNV, jlong index) {
    try {
        auto* nv = getValue(env, thisNV);
        std::string propertyName = NodeVal::getPropertyName(nv, index);
        return utf8StringToJstring(env, propertyName);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuNodeValGetPropertyValueAt(JNIEnv* env, jclass,
    jobject thisNV, jlong index) {
    try {
        auto* nv = getValue(env, thisNV);
        auto propertyValue = NodeVal::getPropertyVal(nv, index);
        jobject ret = createJavaObject(env, propertyValue, J_C_Value, J_C_Value_F_v_ref);
        env->SetBooleanField(ret, J_C_Value_F_isOwnedByCPP, static_cast<jboolean>(true));
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuNodeValToString(JNIEnv* env, jclass,
    jobject thisNV) {
    try {
        auto* nv = getValue(env, thisNV);
        std::string result_string = NodeVal::toString(nv);
        jstring ret = utf8StringToJstring(env, result_string);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuRelValGetId(JNIEnv* env, jclass,
    jobject thisRV) {
    try {
        auto* rv = getValue(env, thisRV);
        auto srcIdVal = RelVal::getIDVal(rv);
        if (srcIdVal == nullptr) {
            return NULL;
        }
        internalID_t id = srcIdVal->getValue<internalID_t>();
        jobject ret = env->NewObject(J_C_InternalID, J_C_InternalID_M_init, id.tableID, id.offset);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuRelValGetSrcId(JNIEnv* env, jclass,
    jobject thisRV) {
    try {
        auto* rv = getValue(env, thisRV);
        auto srcIdVal = RelVal::getSrcNodeIDVal(rv);
        if (srcIdVal == nullptr) {
            return NULL;
        }
        internalID_t id = srcIdVal->getValue<internalID_t>();
        jobject ret = env->NewObject(J_C_InternalID, J_C_InternalID_M_init, id.tableID, id.offset);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuRelValGetDstId(JNIEnv* env, jclass,
    jobject thisRV) {
    try {
        auto* rv = getValue(env, thisRV);
        auto dstIdVal = RelVal::getDstNodeIDVal(rv);
        if (dstIdVal == nullptr) {
            return NULL;
        }
        internalID_t id = dstIdVal->getValue<internalID_t>();
        jobject ret = env->NewObject(J_C_InternalID, J_C_InternalID_M_init, id.tableID, id.offset);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuRelValGetLabelName(JNIEnv* env, jclass,
    jobject thisRV) {
    try {
        auto* rv = getValue(env, thisRV);
        auto labelVal = RelVal::getLabelVal(rv);
        if (labelVal == nullptr) {
            return NULL;
        }
        std::string label = labelVal->getValue<std::string>();
        return utf8StringToJstring(env, label);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuRelValGetPropertySize(JNIEnv* env, jclass,
    jobject thisRV) {
    try {
        auto* rv = getValue(env, thisRV);
        auto size = RelVal::getNumProperties(rv);
        return static_cast<jlong>(size);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuRelValGetPropertyNameAt(JNIEnv* env, jclass,
    jobject thisRV, jlong index) {
    try {
        auto* rv = getValue(env, thisRV);
        std::string name = RelVal::getPropertyName(rv, index);
        return utf8StringToJstring(env, name);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuRelValGetPropertyValueAt(JNIEnv* env, jclass,
    jobject thisRV, jlong index) {
    try {
        auto* rv = getValue(env, thisRV);
        uint64_t idx = static_cast<uint64_t>(index);
        Value* val = RelVal::getPropertyVal(rv, idx);

        jobject ret = createJavaObject(env, val, J_C_Value, J_C_Value_F_v_ref);
        env->SetBooleanField(ret, J_C_Value_F_isOwnedByCPP, static_cast<jboolean>(true));
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuRelValToString(JNIEnv* env, jclass,
    jobject thisRV) {
    try {
        auto* rv = getValue(env, thisRV);
        std::string result_string = RelVal::toString(rv);
        jstring ret = utf8StringToJstring(env, result_string);
        return ret;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuCreateMap(JNIEnv* env, jclass,
    jobjectArray keys, jobjectArray values) {
    try {
        jsize len = env->GetArrayLength(keys);
        KU_ASSERT(env->GetArrayLength(values) == len);
        KU_ASSERT(len > 0);

        std::optional<LogicalType> keyType;
        std::optional<LogicalType> valueType;

        std::vector<std::unique_ptr<Value>> children;
        for (jsize i = 0; i < len; ++i) {
            auto key = getValue(env, env->GetObjectArrayElement(keys, i))->copy();
            auto value = getValue(env, env->GetObjectArrayElement(values, i))->copy();

            if (!keyType.has_value()) {
                keyType = key->getDataType().copy();
                valueType = value->getDataType().copy();
            } else {
                KU_ASSERT(valueType.has_value());
                if (key->getDataType() != *keyType || value->getDataType() != *valueType) {
                    return nullptr;
                }
            }

            std::vector<StructField> structFields;
            structFields.emplace_back(InternalKeyword::MAP_KEY, keyType->copy());
            structFields.emplace_back(InternalKeyword::MAP_VALUE, valueType->copy());

            decltype(children) structVals;
            structVals.emplace_back(std::move(key));
            structVals.emplace_back(std::move(value));
            children.emplace_back(std::make_unique<Value>(
                LogicalType::STRUCT(std::move(structFields)), std::move(structVals)));
        }

        KU_ASSERT(keyType.has_value());
        KU_ASSERT(valueType.has_value());
        Value* mapValue = new Value(LogicalType::MAP(std::move(*keyType), std::move(*valueType)),
            std::move(children));
        return createJavaObject(env, mapValue, J_C_Value, J_C_Value_F_v_ref);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_Native_kuzuCreateStruct(JNIEnv* env, jclass,
    jobjectArray fieldNames, jobjectArray fieldValues) {
    try {
        jsize len = env->GetArrayLength(fieldNames);
        KU_ASSERT(env->GetArrayLength(fieldValues) == len);
        KU_ASSERT(len > 0);

        std::vector<std::unique_ptr<Value>> children;
        auto structFields = std::vector<StructField>{};
        for (jsize i = 0; i < len; ++i) {
            auto fieldName = jstringToUtf8String(env,
                reinterpret_cast<jstring>(env->GetObjectArrayElement(fieldNames, i)));
            auto fieldValue = getValue(env, env->GetObjectArrayElement(fieldValues, i))->copy();
            auto fieldType = fieldValue->getDataType().copy();

            structFields.emplace_back(std::move(fieldName), std::move(fieldType));
            children.push_back(std::move(fieldValue));
        }

        Value* structValue =
            new Value(LogicalType::STRUCT(std::move(structFields)), std::move(children));
        return createJavaObject(env, structValue, J_C_Value, J_C_Value_F_v_ref);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jobject();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuValueGetStructFieldName(JNIEnv* env, jclass,
    jobject thisSV, jlong index) {
    try {
        auto* sv = getValue(env, thisSV);
        const auto& dataType = sv->getDataType();
        auto fieldNames = StructType::getFieldNames(dataType);
        if ((uint64_t)index >= fieldNames.size() || index < 0) {
            return nullptr;
        }
        std::string name = fieldNames[index];
        return utf8StringToJstring(env, name);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuValueGetStructIndex(JNIEnv* env, jclass,
    jobject thisSV, jstring fieldName) {
    try {
        auto* sv = getValue(env, thisSV);
        std::string field_name_str = jstringToUtf8String(env, fieldName);
        const auto& dataType = sv->getDataType();
        auto index = StructType::getFieldIdx(dataType, field_name_str);
        if (index == INVALID_STRUCT_FIELD_IDX) {
            return -1;
        } else {
            return static_cast<jlong>(index);
        }
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_Native_kuzuGetVersion(JNIEnv* env, jclass) {
    try {
        return env->NewStringUTF(Version::getVersion());

    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jstring();
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_Native_kuzuGetStorageVersion(JNIEnv* env, jclass) {
    try {
        return static_cast<jlong>(Version::getStorageVersion());

    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jlong();
}

void createGlobalClassRef(JNIEnv* env, jclass& globalClassRef, const char* className) {
    try {
        jclass tempLocalClassRef = env->FindClass(className);
        globalClassRef = (jclass)env->NewGlobalRef(tempLocalClassRef);
        env->DeleteLocalRef(tempLocalClassRef);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

void initGlobalClassRef(JNIEnv* env) {
    try {
        createGlobalClassRef(env, J_C_Map, "java/util/Map");

        createGlobalClassRef(env, J_C_Set, "java/util/Set");

        createGlobalClassRef(env, J_C_Iterator, "java/util/Iterator");

        createGlobalClassRef(env, J_C_Map$Entry, "java/util/Map$Entry");

        createGlobalClassRef(env, J_C_Exception, "java/lang/Exception");

        createGlobalClassRef(env, J_C_QueryResult, "com/kuzudb/QueryResult");

        createGlobalClassRef(env, J_C_PreparedStatement, "com/kuzudb/PreparedStatement");

        createGlobalClassRef(env, J_C_DataType, "com/kuzudb/DataType");

        createGlobalClassRef(env, J_C_QuerySummary, "com/kuzudb/QuerySummary");

        createGlobalClassRef(env, J_C_FlatTuple, "com/kuzudb/FlatTuple");

        createGlobalClassRef(env, J_C_Value, "com/kuzudb/Value");

        createGlobalClassRef(env, J_C_DataTypeID, "com/kuzudb/DataTypeID");

        createGlobalClassRef(env, J_C_Boolean, "java/lang/Boolean");

        createGlobalClassRef(env, J_C_Long, "java/lang/Long");

        createGlobalClassRef(env, J_C_Integer, "java/lang/Integer");

        createGlobalClassRef(env, J_C_InternalID, "com/kuzudb/InternalID");

        createGlobalClassRef(env, J_C_Double, "java/lang/Double");

        createGlobalClassRef(env, J_C_BigDecimal, "java/math/BigDecimal");

        createGlobalClassRef(env, J_C_LocalDate, "java/time/LocalDate");

        createGlobalClassRef(env, J_C_Instant, "java/time/Instant");

        createGlobalClassRef(env, J_C_Short, "java/lang/Short");

        createGlobalClassRef(env, J_C_Byte, "java/lang/Byte");

        createGlobalClassRef(env, J_C_BigInteger, "java/math/BigInteger");

        createGlobalClassRef(env, J_C_Float, "java/lang/Float");

        createGlobalClassRef(env, J_C_Duration, "java/time/Duration");

        createGlobalClassRef(env, J_C_UUID, "java/util/UUID");

        createGlobalClassRef(env, J_C_Connection, "com/kuzudb/Connection");

        createGlobalClassRef(env, J_C_Database, "com/kuzudb/Database");

        createGlobalClassRef(env, J_C_String, "java/lang/String");
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

void initGlobalMethodRef(JNIEnv* env) {
    try {
        J_C_Map_M_entrySet = env->GetMethodID(J_C_Map, "entrySet", "()Ljava/util/Set;");

        J_C_Set_M_iterator = env->GetMethodID(J_C_Set, "iterator", "()Ljava/util/Iterator;");

        J_C_Iterator_M_hasNext = env->GetMethodID(J_C_Iterator, "hasNext", "()Z");
        J_C_Iterator_M_next = env->GetMethodID(J_C_Iterator, "next", "()Ljava/lang/Object;");

        J_C_Map$Entry_M_getKey = env->GetMethodID(J_C_Map$Entry, "getKey", "()Ljava/lang/Object;");
        J_C_Map$Entry_M_getValue =
            env->GetMethodID(J_C_Map$Entry, "getValue", "()Ljava/lang/Object;");

        J_C_QuerySummary_M_ctor = env->GetMethodID(J_C_QuerySummary, "<init>", "(DD)V");

        J_C_Boolean_M_init = env->GetMethodID(J_C_Boolean, "<init>", "(Z)V");

        J_C_Long_M_init = env->GetMethodID(J_C_Long, "<init>", "(J)V");

        J_C_Integer_M_init = env->GetMethodID(J_C_Integer, "<init>", "(I)V");

        J_C_InternalID_M_init = env->GetMethodID(J_C_InternalID, "<init>", "(JJ)V");

        J_C_Double_M_init = env->GetMethodID(J_C_Double, "<init>", "(D)V");

        J_C_BigDecimal_M_init = env->GetMethodID(J_C_BigDecimal, "<init>", "(Ljava/lang/String;)V");

        J_C_LocalDate_M_ofEpochDay =
            env->GetStaticMethodID(J_C_LocalDate, "ofEpochDay", "(J)Ljava/time/LocalDate;");
        J_C_LocalDate_M_toEpochDay = env->GetMethodID(J_C_LocalDate, "toEpochDay", "()J");
        J_C_LocalDate_M_getEpochSecond = env->GetMethodID(J_C_Instant, "getEpochSecond", "()J");
        J_C_LocalDate_M_getNano = env->GetMethodID(J_C_Instant, "getNano", "()I");

        J_C_Instant_M_ofEpochSecond =
            env->GetStaticMethodID(J_C_Instant, "ofEpochSecond", "(JJ)Ljava/time/Instant;");

        J_C_Short_M_init = env->GetMethodID(J_C_Short, "<init>", "(S)V");

        J_C_Float_M_init = env->GetMethodID(J_C_Float, "<init>", "(F)V");

        J_C_Duration_M_ofMillis =
            env->GetStaticMethodID(J_C_Duration, "ofMillis", "(J)Ljava/time/Duration;");
        J_C_Duration_M_toMillis = env->GetMethodID(J_C_Duration, "toMillis", "()J");

        J_C_Byte_M_init = env->GetMethodID(J_C_Byte, "<init>", "(B)V");

        J_C_BigInteger_M_init = env->GetMethodID(J_C_BigInteger, "<init>", "(Ljava/lang/String;)V");

        J_C_BigInteger_M_longValue = env->GetMethodID(J_C_BigInteger, "longValue", "()J");

        J_C_BigInteger_M_shiftRight =
            env->GetMethodID(J_C_BigInteger, "shiftRight", "(I)Ljava/math/BigInteger;");

        J_C_UUID_M_init = env->GetMethodID(J_C_UUID, "<init>", "(JJ)V");

        J_C_UUID_M_getMostSignificantBits =
            env->GetMethodID(J_C_UUID, "getMostSignificantBits", "()J");

        J_C_UUID_M_getLeastSignificantBits =
            env->GetMethodID(J_C_UUID, "getLeastSignificantBits", "()J");

        J_C_Boolean_M_booleanValue = env->GetMethodID(J_C_Boolean, "booleanValue", "()Z");

        J_C_Byte_M_byteValue = env->GetMethodID(J_C_Byte, "byteValue", "()B");

        J_C_Short_M_shortValue = env->GetMethodID(J_C_Short, "shortValue", "()S");

        J_C_Integer_M_intValue = env->GetMethodID(J_C_Integer, "intValue", "()I");

        J_C_Long_M_longValue = env->GetMethodID(J_C_Long, "longValue", "()J");

        J_C_Float_M_floatValue = env->GetMethodID(J_C_Float, "floatValue", "()F");

        J_C_Double_M_doubleValue = env->GetMethodID(J_C_Double, "doubleValue", "()D");

        J_C_BigDecimal_M_toString =
            env->GetMethodID(J_C_BigDecimal, "toString", "()Ljava/lang/String;");

        J_C_BigDecimal_M_precision = env->GetMethodID(J_C_BigDecimal, "precision", "()I");

        J_C_BigDecimal_M_scale = env->GetMethodID(J_C_BigDecimal, "scale", "()I");

        J_C_String_M_ctor = env->GetMethodID(J_C_String, "<init>", "([BLjava/lang/String;)V");

        J_C_String_M_getBytes = env->GetMethodID(J_C_String, "getBytes", "(Ljava/lang/String;)[B");
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

void initGlobalFieldRef(JNIEnv* env) {
    try {
        J_C_QueryResult_F_qr_ref = env->GetFieldID(J_C_QueryResult, "qr_ref", "J");
        J_C_QueryResult_F_isOwnedByCPP = env->GetFieldID(J_C_QueryResult, "isOwnedByCPP", "Z");

        J_C_PreparedStatement_F_ps_ref = env->GetFieldID(J_C_PreparedStatement, "ps_ref", "J");

        J_C_DataType_F_dt_ref = env->GetFieldID(J_C_DataType, "dt_ref", "J");

        J_C_FlatTuple_F_ft_ref = env->GetFieldID(J_C_FlatTuple, "ft_ref", "J");

        J_C_Value_F_v_ref = env->GetFieldID(J_C_Value, "v_ref", "J");
        J_C_Value_F_isOwnedByCPP = env->GetFieldID(J_C_Value, "isOwnedByCPP", "Z");

        J_C_DataTypeID_F_value = env->GetFieldID(J_C_DataTypeID, "value", "I");

        J_C_InternalID_F_tableId = env->GetFieldID(J_C_InternalID, "tableId", "J");

        J_C_InternalID_F_offset = env->GetFieldID(J_C_InternalID, "offset", "J");

        J_C_Connection_F_conn_ref = env->GetFieldID(J_C_Connection, "conn_ref", "J");

        J_C_Database_db_ref = env->GetFieldID(J_C_Database, "db_ref", "J");
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* /*reserved*/) {
    JNIEnv* env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }
    try {
        initGlobalClassRef(env);
        initGlobalMethodRef(env);
        initGlobalFieldRef(env);
        return JNI_VERSION;
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
    return jint();
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* /*reserved*/) {
    JNIEnv* env = nullptr;
    vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

    try {
        env->DeleteGlobalRef(J_C_Map);
        env->DeleteGlobalRef(J_C_Set);
        env->DeleteGlobalRef(J_C_Iterator);
        env->DeleteGlobalRef(J_C_Map$Entry);
        env->DeleteGlobalRef(J_C_Exception);
        env->DeleteGlobalRef(J_C_QueryResult);
        env->DeleteGlobalRef(J_C_PreparedStatement);
        env->DeleteGlobalRef(J_C_DataType);
        env->DeleteGlobalRef(J_C_QuerySummary);
        env->DeleteGlobalRef(J_C_FlatTuple);
        env->DeleteGlobalRef(J_C_Value);
        env->DeleteGlobalRef(J_C_DataTypeID);
        env->DeleteGlobalRef(J_C_Boolean);
        env->DeleteGlobalRef(J_C_Long);
        env->DeleteGlobalRef(J_C_Integer);
        env->DeleteGlobalRef(J_C_InternalID);
        env->DeleteGlobalRef(J_C_Double);
        env->DeleteGlobalRef(J_C_BigDecimal);
        env->DeleteGlobalRef(J_C_LocalDate);
        env->DeleteGlobalRef(J_C_Instant);
        env->DeleteGlobalRef(J_C_Short);
        env->DeleteGlobalRef(J_C_Byte);
        env->DeleteGlobalRef(J_C_BigInteger);
        env->DeleteGlobalRef(J_C_Float);
        env->DeleteGlobalRef(J_C_Duration);
        env->DeleteGlobalRef(J_C_UUID);
        env->DeleteGlobalRef(J_C_Connection);
        env->DeleteGlobalRef(J_C_Database);
        env->DeleteGlobalRef(J_C_String);
    } catch (const Exception& e) {
        throwJNIException(env, e.what());
    } catch (...) {
        throwJNIException(env, "Unknown Error");
    }
}
