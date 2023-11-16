#include <unordered_map>

// This header is generated at build time. See CMakeLists.txt.
#include "com_kuzudb_KuzuNative.h"
#include "common/exception/conversion.h"
#include "common/exception/exception.h"
#include "common/types/types.h"
#include "common/types/value/nested.h"
#include "common/types/value/node.h"
#include "common/types/value/rel.h"
#include "common/types/value/value.h"
#include "main/kuzu.h"
#include "main/query_summary.h"
#include <jni.h>

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;

jobject createJavaObject(
    JNIEnv* env, void* memAddress, std::string classPath, std::string refFieldName) {
    auto address = reinterpret_cast<uint64_t>(memAddress);
    auto ref = static_cast<jlong>(address);

    jclass javaClass = env->FindClass(classPath.c_str());
    jobject newObject = env->AllocObject(javaClass);
    jfieldID refID = env->GetFieldID(javaClass, refFieldName.c_str(), "J");
    env->SetLongField(newObject, refID, ref);
    return newObject;
}

Database* getDatabase(JNIEnv* env, jobject thisDB) {
    jclass javaDBClass = env->GetObjectClass(thisDB);
    jfieldID fieldID = env->GetFieldID(javaDBClass, "db_ref", "J");
    jlong fieldValue = env->GetLongField(thisDB, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Database* db = reinterpret_cast<Database*>(address);
    return db;
}

Connection* getConnection(JNIEnv* env, jobject thisConn) {
    jclass javaConnClass = env->GetObjectClass(thisConn);
    jfieldID fieldID = env->GetFieldID(javaConnClass, "conn_ref", "J");
    jlong fieldValue = env->GetLongField(thisConn, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Connection* conn = reinterpret_cast<Connection*>(address);
    return conn;
}

PreparedStatement* getPreparedStatement(JNIEnv* env, jobject thisPS) {
    jclass javaPSClass = env->GetObjectClass(thisPS);
    jfieldID fieldID = env->GetFieldID(javaPSClass, "ps_ref", "J");
    jlong fieldValue = env->GetLongField(thisPS, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    PreparedStatement* ps = reinterpret_cast<PreparedStatement*>(address);
    return ps;
}

QueryResult* getQueryResult(JNIEnv* env, jobject thisQR) {
    jclass javaQRClass = env->GetObjectClass(thisQR);
    jfieldID fieldID = env->GetFieldID(javaQRClass, "qr_ref", "J");
    jlong fieldValue = env->GetLongField(thisQR, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    QueryResult* qr = reinterpret_cast<QueryResult*>(address);
    return qr;
}

FlatTuple* getFlatTuple(JNIEnv* env, jobject thisFT) {
    jclass javaFTClass = env->GetObjectClass(thisFT);
    jfieldID fieldID = env->GetFieldID(javaFTClass, "ft_ref", "J");
    jlong fieldValue = env->GetLongField(thisFT, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    auto ft = reinterpret_cast<std::shared_ptr<FlatTuple>*>(address);
    return ft->get();
}

LogicalType* getDataType(JNIEnv* env, jobject thisDT) {
    jclass javaDTClass = env->GetObjectClass(thisDT);
    jfieldID fieldID = env->GetFieldID(javaDTClass, "dt_ref", "J");
    jlong fieldValue = env->GetLongField(thisDT, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    auto* dt = reinterpret_cast<LogicalType*>(address);
    return dt;
}

Value* getValue(JNIEnv* env, jobject thisValue) {
    jclass javaValueClass = env->GetObjectClass(thisValue);
    jfieldID fieldID = env->GetFieldID(javaValueClass, "v_ref", "J");
    jlong fieldValue = env->GetLongField(thisValue, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Value* v = reinterpret_cast<Value*>(address);
    return v;
}

internalID_t getInternalID(JNIEnv* env, jobject id) {
    jclass javaIDClass = env->GetObjectClass(id);
    jfieldID fieldID = env->GetFieldID(javaIDClass, "table_id", "J");
    long table_id = static_cast<long>(env->GetLongField(id, fieldID));
    fieldID = env->GetFieldID(javaIDClass, "offset", "J");
    long offset = static_cast<long>(env->GetLongField(id, fieldID));
    return internalID_t(offset, table_id);
}

std::string dataTypeToString(const LogicalType& dataType) {
    return LogicalTypeUtils::toString(dataType.getLogicalTypeID());
}

std::unordered_map<std::string, std::unique_ptr<Value>> javaMapToCPPMap(
    JNIEnv* env, jobject javaMap) {

    jclass mapClass = env->FindClass("java/util/Map");
    jmethodID entrySet = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");
    jobject set = env->CallObjectMethod(javaMap, entrySet);
    jclass setClass = env->FindClass("java/util/Set");
    jmethodID iterator = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
    jobject iter = env->CallObjectMethod(set, iterator);
    jclass iteratorClass = env->FindClass("java/util/Iterator");
    jmethodID hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID next = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
    jclass entryClass = env->FindClass("java/util/Map$Entry");
    jmethodID entryGetKey = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
    jmethodID entryGetValue = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");

    std::unordered_map<std::string, std::unique_ptr<Value>> result;
    while (env->CallBooleanMethod(iter, hasNext)) {
        jobject entry = env->CallObjectMethod(iter, next);
        jstring key = (jstring)env->CallObjectMethod(entry, entryGetKey);
        jobject value = env->CallObjectMethod(entry, entryGetValue);
        const char* keyStr = env->GetStringUTFChars(key, JNI_FALSE);
        const Value* v = getValue(env, value);
        // Java code can keep a reference to the value, so we cannot move.
        result.insert({keyStr, v->copy()});

        env->DeleteLocalRef(entry);
        env->ReleaseStringUTFChars(key, keyStr);
        env->DeleteLocalRef(key);
        env->DeleteLocalRef(value);
    }
    return result;
}

/**
 * All Database native functions
 */

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1database_1init(JNIEnv* env, jclass,
    jstring database_path, jlong buffer_pool_size, jboolean enable_compression,
    jboolean read_only) {

    const char* path = env->GetStringUTFChars(database_path, JNI_FALSE);
    uint64_t buffer = static_cast<uint64_t>(buffer_pool_size);
    SystemConfig systemConfig;
    systemConfig.bufferPoolSize = buffer == 0 ? -1u : buffer;
    systemConfig.enableCompression = enable_compression;
    systemConfig.readOnly = read_only;
    try {
        Database* db = new Database(path, systemConfig);
        uint64_t address = reinterpret_cast<uint64_t>(db);

        env->ReleaseStringUTFChars(database_path, path);
        return static_cast<jlong>(address);
    } catch (Exception& e) {
        env->ReleaseStringUTFChars(database_path, path);
        jclass Exception = env->FindClass("java/lang/Exception");
        env->ThrowNew(Exception, e.what());
    }
    return 0;
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1database_1destroy(
    JNIEnv* env, jclass, jobject thisDB) {
    Database* db = getDatabase(env, thisDB);
    delete db;
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1database_1set_1logging_1level(
    JNIEnv* env, jclass, jstring logging_level) {
    const char* lvl = env->GetStringUTFChars(logging_level, JNI_FALSE);
    try {
        Database::setLoggingLevel(lvl);
        env->ReleaseStringUTFChars(logging_level, lvl);
    } catch (ConversionException e) {
        env->ReleaseStringUTFChars(logging_level, lvl);
        jclass Exception = env->FindClass("java/lang/Exception");
        env->ThrowNew(Exception, e.what());
    }
}

/**
 * All Connection native functions
 */

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1init(
    JNIEnv* env, jclass, jobject db) {

    try {
        Database* conn_db = getDatabase(env, db);

        Connection* conn = new Connection(conn_db);
        uint64_t connAddress = reinterpret_cast<uint64_t>(conn);

        return static_cast<jlong>(connAddress);
    } catch (Exception& e) {
        jclass Exception = env->FindClass("java/lang/Exception");
        env->ThrowNew(Exception, e.what());
    }
    return 0;
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1destroy(
    JNIEnv* env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    delete conn;
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1begin_1read_1only_1transaction(
    JNIEnv* env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->beginReadOnlyTransaction();
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1begin_1write_1transaction(
    JNIEnv* env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->beginWriteTransaction();
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1commit(
    JNIEnv* env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->commit();
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1rollback(
    JNIEnv* env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->rollback();
}

JNIEXPORT void JNICALL
Java_com_kuzudb_KuzuNative_kuzu_1connection_1set_1max_1num_1thread_1for_1exec(
    JNIEnv* env, jclass, jobject thisConn, jlong num_threads) {
    Connection* conn = getConnection(env, thisConn);
    uint64_t threads = static_cast<uint64_t>(num_threads);
    conn->setMaxNumThreadForExec(threads);
}

JNIEXPORT jlong JNICALL
Java_com_kuzudb_KuzuNative_kuzu_1connection_1get_1max_1num_1thread_1for_1exec(
    JNIEnv* env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    uint64_t threads = conn->getMaxNumThreadForExec();
    jlong num_threads = static_cast<jlong>(threads);
    return num_threads;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1query(
    JNIEnv* env, jclass, jobject thisConn, jstring query) {
    Connection* conn = getConnection(env, thisConn);
    const char* CPPQuery = env->GetStringUTFChars(query, JNI_FALSE);
    auto query_result = conn->query(CPPQuery).release();
    env->ReleaseStringUTFChars(query, CPPQuery);

    uint64_t qrAddress = reinterpret_cast<uint64_t>(query_result);
    jlong qr_ref = static_cast<jlong>(qrAddress);

    jclass qrClass = env->FindClass("com/kuzudb/KuzuQueryResult");
    jobject newQRObject = env->AllocObject(qrClass);
    jfieldID refID = env->GetFieldID(qrClass, "qr_ref", "J");
    env->SetLongField(newQRObject, refID, qr_ref);
    return newQRObject;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1prepare(
    JNIEnv* env, jclass, jobject thisConn, jstring query) {
    Connection* conn = getConnection(env, thisConn);
    const char* cppquery = env->GetStringUTFChars(query, JNI_FALSE);

    PreparedStatement* prepared_statement = conn->prepare(cppquery).release();
    env->ReleaseStringUTFChars(query, cppquery);
    if (prepared_statement == nullptr) {
        return nullptr;
    }

    jobject ret =
        createJavaObject(env, prepared_statement, "com/kuzudb/KuzuPreparedStatement", "ps_ref");
    return ret;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1execute(
    JNIEnv* env, jclass, jobject thisConn, jobject preStm, jobject param_map) {
    Connection* conn = getConnection(env, thisConn);
    PreparedStatement* ps = getPreparedStatement(env, preStm);

    std::unordered_map<std::string, std::unique_ptr<Value>> params =
        javaMapToCPPMap(env, param_map);

    auto query_result = conn->executeWithParams(ps, std::move(params)).release();
    if (query_result == nullptr) {
        return nullptr;
    }

    jobject ret = createJavaObject(env, query_result, "com/kuzudb/KuzuQueryResult", "qr_ref");
    return ret;
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1interrupt(
    JNIEnv* env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->interrupt();
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1connection_1set_1query_1timeout(
    JNIEnv* env, jclass, jobject thisConn, jlong timeout_in_ms) {
    Connection* conn = getConnection(env, thisConn);
    uint64_t timeout = static_cast<uint64_t>(timeout_in_ms);
    conn->setQueryTimeOut(timeout);
}

/**
 * All PreparedStatement native functions
 */

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1prepared_1statement_1destroy(
    JNIEnv* env, jclass, jobject thisPS) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    delete ps;
}

JNIEXPORT jboolean JNICALL
Java_com_kuzudb_KuzuNative_kuzu_1prepared_1statement_1allow_1active_1transaction(
    JNIEnv* env, jclass, jobject thisPS) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    return static_cast<jboolean>(ps->allowActiveTransaction());
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_KuzuNative_kuzu_1prepared_1statement_1is_1success(
    JNIEnv* env, jclass, jobject thisPS) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    return static_cast<jboolean>(ps->isSuccess());
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1prepared_1statement_1get_1error_1message(
    JNIEnv* env, jclass, jobject thisPS) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string errorMessage = ps->getErrorMessage();
    jstring msg = env->NewStringUTF(errorMessage.c_str());
    return msg;
}

/**
 * All QueryResult native functions
 */

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1destroy(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    delete qr;
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1is_1success(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    return static_cast<jboolean>(qr->isSuccess());
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1get_1error_1message(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    std::string errorMessage = qr->getErrorMessage();
    jstring msg = env->NewStringUTF(errorMessage.c_str());
    return msg;
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1get_1num_1columns(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    return static_cast<jlong>(qr->getNumColumns());
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1get_1column_1name(
    JNIEnv* env, jclass, jobject thisQR, jlong index) {
    QueryResult* qr = getQueryResult(env, thisQR);
    auto column_names = qr->getColumnNames();
    uint64_t idx = static_cast<uint64_t>(index);
    if (idx >= column_names.size()) {
        return nullptr;
    }
    std::string column_name = column_names[idx];
    jstring name = env->NewStringUTF(column_name.c_str());
    return name;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1get_1column_1data_1type(
    JNIEnv* env, jclass, jobject thisQR, jlong index) {
    QueryResult* qr = getQueryResult(env, thisQR);
    auto column_datatypes = qr->getColumnDataTypes();
    uint64_t idx = static_cast<uint64_t>(index);
    if (idx >= column_datatypes.size()) {
        return nullptr;
    }
    auto column_datatype = column_datatypes[idx];
    auto* cdt_copy = new LogicalType(column_datatype);

    uint64_t dtAddress = reinterpret_cast<uint64_t>(cdt_copy);
    jlong dt_ref = static_cast<jlong>(dtAddress);

    jclass dtClass = env->FindClass("com/kuzudb/KuzuDataType");
    jobject newDTObject = env->AllocObject(dtClass);
    jfieldID refID = env->GetFieldID(dtClass, "dt_ref", "J");
    env->SetLongField(newDTObject, refID, dt_ref);
    return newDTObject;
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1get_1num_1tuples(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    return static_cast<jlong>(qr->getNumTuples());
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1get_1query_1summary(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    auto query_summary = qr->getQuerySummary();

    jdouble cmpTime = static_cast<jdouble>(query_summary->getCompilingTime());
    jdouble exeTime = static_cast<jdouble>(query_summary->getExecutionTime());

    jclass qsClass = env->FindClass("com/kuzudb/KuzuQuerySummary");
    jmethodID ctor = env->GetMethodID(qsClass, "<init>", "(DD)V");
    jobject newQSObject = env->NewObject(qsClass, ctor, cmpTime, exeTime);
    return newQSObject;
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1has_1next(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    return static_cast<jboolean>(qr->hasNext());
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1get_1next(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    auto flat_tuple = qr->getNext();

    auto newFT = new std::shared_ptr<FlatTuple>(flat_tuple);
    uint64_t ftAddress = reinterpret_cast<uint64_t>(newFT);
    jlong ft_ref = static_cast<jlong>(ftAddress);

    jclass ftClass = env->FindClass("com/kuzudb/KuzuFlatTuple");
    jobject newFTObject = env->AllocObject(ftClass);
    jfieldID refID = env->GetFieldID(ftClass, "ft_ref", "J");
    env->SetLongField(newFTObject, refID, ft_ref);
    return newFTObject;
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1to_1string(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    std::string result_string = qr->toString();
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1write_1to_1csv(JNIEnv* env,
    jclass, jobject thisQR, jstring file_path, jchar delimiter, jchar escape_char, jchar new_line) {
    QueryResult* qr = getQueryResult(env, thisQR);
    const char* cpp_file_path = env->GetStringUTFChars(file_path, JNI_FALSE);

    // TODO: confirm this convertion is ok to do.
    // jchar is 16-bit unicode character so converting to char will lose the higher oreder-bits
    char cpp_delimiter = static_cast<char>(delimiter);
    char cpp_escape_char = static_cast<char>(escape_char);
    char cpp_new_line = static_cast<char>(new_line);

    qr->writeToCSV(cpp_file_path, cpp_delimiter, cpp_escape_char, cpp_new_line);
    env->ReleaseStringUTFChars(file_path, cpp_file_path);
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1query_1result_1reset_1iterator(
    JNIEnv* env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    qr->resetIterator();
}

/**
 * All FlatTuple native functions
 */

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1flat_1tuple_1destroy(
    JNIEnv* env, jclass, jobject thisFT) {
    jclass javaFTClass = env->GetObjectClass(thisFT);
    jfieldID fieldID = env->GetFieldID(javaFTClass, "ft_ref", "J");
    jlong fieldValue = env->GetLongField(thisFT, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    auto flat_tuple_shared_ptr = reinterpret_cast<std::shared_ptr<FlatTuple>*>(address);

    flat_tuple_shared_ptr->reset();
    delete flat_tuple_shared_ptr;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1flat_1tuple_1get_1value(
    JNIEnv* env, jclass, jobject thisFT, jlong index) {
    FlatTuple* ft = getFlatTuple(env, thisFT);
    uint32_t idx = static_cast<uint32_t>(index);
    Value* value;
    try {
        value = ft->getValue(index);
    } catch (Exception& e) { return nullptr; }

    jobject v = createJavaObject(env, value, "com/kuzudb/KuzuValue", "v_ref");
    jclass clazz = env->GetObjectClass(v);
    jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
    env->SetBooleanField(v, fieldID, static_cast<jboolean>(true));

    return v;
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1flat_1tuple_1to_1string(
    JNIEnv* env, jclass, jobject thisFT) {
    FlatTuple* ft = getFlatTuple(env, thisFT);
    std::string result_string = ft->toString();
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

/**
 * All DataType native functions
 */

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1data_1type_1create(
    JNIEnv* env, jclass, jobject id, jobject child_type, jlong fixed_num_elements_in_list) {
    jclass javaIDClass = env->GetObjectClass(id);
    jfieldID fieldID = env->GetFieldID(javaIDClass, "value", "I");
    jint fieldValue = env->GetIntField(id, fieldID);

    uint8_t data_type_id_u8 = static_cast<uint8_t>(fieldValue);
    LogicalType* data_type;
    auto logicalTypeID = static_cast<LogicalTypeID>(data_type_id_u8);
    if (child_type == nullptr) {
        data_type = new LogicalType(logicalTypeID);
    } else {
        auto child_type_pty = std::make_unique<LogicalType>(*getDataType(env, child_type));
        auto extraTypeInfo = fixed_num_elements_in_list > 0 ?
                                 std::make_unique<FixedListTypeInfo>(
                                     std::move(child_type_pty), fixed_num_elements_in_list) :
                                 std::make_unique<VarListTypeInfo>(std::move(child_type_pty));
        data_type = new LogicalType(logicalTypeID, std::move(extraTypeInfo));
    }
    uint64_t address = reinterpret_cast<uint64_t>(data_type);
    return static_cast<jlong>(address);
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1data_1type_1clone(
    JNIEnv* env, jclass, jobject thisDT) {
    auto* oldDT = getDataType(env, thisDT);
    auto* newDT = new LogicalType(*oldDT);

    jobject dt = createJavaObject(env, newDT, "com/kuzudb/KuzuDataType", "dt_ref");
    return dt;
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1data_1type_1destroy(
    JNIEnv* env, jclass, jobject thisDT) {
    auto* dt = getDataType(env, thisDT);
    delete dt;
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_KuzuNative_kuzu_1data_1type_1equals(
    JNIEnv* env, jclass, jobject dt1, jobject dt2) {
    auto* cppdt1 = getDataType(env, dt1);
    auto* cppdt2 = getDataType(env, dt2);

    return static_cast<jboolean>(*cppdt1 == *cppdt2);
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1data_1type_1get_1id(
    JNIEnv* env, jclass, jobject thisDT) {

    auto* dt = getDataType(env, thisDT);
    std::string id_str = dataTypeToString(*dt);
    jclass idClass = env->FindClass("com/kuzudb/KuzuDataTypeID");
    jfieldID idField =
        env->GetStaticFieldID(idClass, id_str.c_str(), "Lcom/kuzudb/KuzuDataTypeID;");
    jobject id = env->GetStaticObjectField(idClass, idField);
    return id;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1data_1type_1get_1child_1type(
    JNIEnv* env, jclass, jobject thisDT) {
    auto* parent_type = getDataType(env, thisDT);
    LogicalType* child_type;
    if (parent_type->getLogicalTypeID() == LogicalTypeID::FIXED_LIST) {
        child_type = FixedListType::getChildType(parent_type);
    } else if (parent_type->getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
        child_type = VarListType::getChildType(parent_type);
    } else {
        return nullptr;
    }
    auto* new_child_type = new LogicalType(*child_type);
    jobject ret = createJavaObject(env, new_child_type, "com/kuzudb/KuzuDataType", "dt_ref");
    return ret;
}

JNIEXPORT jlong JNICALL
Java_com_kuzudb_KuzuNative_kuzu_1data_1type_1get_1fixed_1num_1elements_1in_1list(
    JNIEnv* env, jclass, jobject thisDT) {
    auto* dt = getDataType(env, thisDT);
    if (dt->getLogicalTypeID() != LogicalTypeID::FIXED_LIST) {
        return 0;
    }
    return static_cast<jlong>(FixedListType::getNumValuesInList(dt));
}

/**
 * All Value native functions
 */

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1create_1null(
    JNIEnv* env, jclass) {
    Value* v = new Value(Value::createNullValue());
    jobject ret = createJavaObject(env, v, "com/kuzudb/KuzuValue", "v_ref");
    return ret;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1create_1null_1with_1data_1type(
    JNIEnv* env, jclass, jobject data_type) {
    auto* dt = getDataType(env, data_type);
    Value* v = new Value(Value::createNullValue(*dt));
    jobject ret = createJavaObject(env, v, "com/kuzudb/KuzuValue", "v_ref");
    return ret;
}

JNIEXPORT jboolean JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1is_1null(
    JNIEnv* env, jclass, jobject thisV) {
    Value* v = getValue(env, thisV);
    return static_cast<jboolean>(v->isNull());
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1set_1null(
    JNIEnv* env, jclass, jobject thisV, jboolean is_null) {
    Value* v = getValue(env, thisV);
    v->setNull(static_cast<bool>(is_null));
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1create_1default(
    JNIEnv* env, jclass, jobject data_type) {
    auto* dt = getDataType(env, data_type);
    Value* v = new Value(Value::createDefaultValue(*dt));
    jobject ret = createJavaObject(env, v, "com/kuzudb/KuzuValue", "v_ref");
    return ret;
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1create_1value(
    JNIEnv* env, jclass, jobject val) {
    Value* v;
    jclass val_class = env->GetObjectClass(val);
    if (env->IsInstanceOf(val, env->FindClass("java/lang/Boolean"))) {
        jboolean value =
            env->CallBooleanMethod(val, env->GetMethodID(val_class, "booleanValue", "()Z"));
        v = new Value(static_cast<bool>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Short"))) {
        jshort value = env->CallShortMethod(val, env->GetMethodID(val_class, "shortValue", "()S"));
        v = new Value(static_cast<int16_t>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Integer"))) {
        jint value = env->CallIntMethod(val, env->GetMethodID(val_class, "intValue", "()I"));
        v = new Value(static_cast<int32_t>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Long"))) {
        jlong value = env->CallLongMethod(val, env->GetMethodID(val_class, "longValue", "()J"));
        v = new Value(static_cast<int64_t>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Float"))) {
        jfloat value = env->CallFloatMethod(val, env->GetMethodID(val_class, "floatValue", "()F"));
        v = new Value(static_cast<float>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Double"))) {
        jdouble value =
            env->CallDoubleMethod(val, env->GetMethodID(val_class, "doubleValue", "()D"));
        v = new Value(static_cast<double>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/String"))) {
        jstring value = static_cast<jstring>(val);
        const char* str = env->GetStringUTFChars(value, JNI_FALSE);
        v = new Value(str);
        env->ReleaseStringUTFChars(value, str);
    } else if (env->IsInstanceOf(val, env->FindClass("com/kuzudb/KuzuInternalID"))) {
        jfieldID fieldID = env->GetFieldID(val_class, "tableId", "J");
        long table_id = static_cast<long>(env->GetLongField(val, fieldID));
        fieldID = env->GetFieldID(val_class, "offset", "J");
        long offset = static_cast<long>(env->GetLongField(val, fieldID));
        internalID_t id(offset, table_id);
        v = new Value(id);
    } else if (env->IsInstanceOf(val, env->FindClass("java/time/LocalDate"))) {
        jmethodID toEpochDay = env->GetMethodID(val_class, "toEpochDay", "()J");
        long days = static_cast<long>(env->CallLongMethod(val, toEpochDay));
        v = new Value(date_t(days));
    } else if (env->IsInstanceOf(val, env->FindClass("java/time/Instant"))) {
        // TODO: Need to review this for overflow
        jmethodID getEpochSecond = env->GetMethodID(val_class, "getEpochSecond", "()J");
        jmethodID getNano = env->GetMethodID(val_class, "getNano", "()I");
        long seconds = static_cast<long>(env->CallLongMethod(val, getEpochSecond));
        long nano = static_cast<long>(env->CallLongMethod(val, getNano));

        long micro = (seconds * 1000000L) + (nano / 1000L);
        v = new Value(timestamp_t(micro));
    } else if (env->IsInstanceOf(val, env->FindClass("java/time/Duration"))) {
        jmethodID toMillis = env->GetMethodID(val_class, "toMillis", "()J");
        auto milis = env->CallLongMethod(val, toMillis);
        v = new Value(interval_t(0, 0, milis * 1000L));
    } else {
        // Throw exception here
        return -1;
    }
    uint64_t address = reinterpret_cast<uint64_t>(v);
    return static_cast<jlong>(address);
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1clone(
    JNIEnv* env, jclass, jobject thisValue) {
    Value* v = getValue(env, thisValue);
    Value* copy = new Value(*v);
    return createJavaObject(env, copy, "com/kuzudb/KuzuValue", "v_ref");
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1copy(
    JNIEnv* env, jclass, jobject thisValue, jobject otherValue) {
    Value* thisV = getValue(env, thisValue);
    Value* otherV = getValue(env, otherValue);
    thisV->copyValueFrom(*otherV);
}

JNIEXPORT void JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1destroy(
    JNIEnv* env, jclass, jobject thisValue) {
    Value* v = getValue(env, thisValue);
    delete v;
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1get_1list_1size(
    JNIEnv* env, jclass, jobject thisValue) {
    Value* v = getValue(env, thisValue);
    return static_cast<jlong>(NestedVal::getChildrenSize(v));
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1get_1list_1element(
    JNIEnv* env, jclass, jobject thisValue, jlong index) {
    Value* v = getValue(env, thisValue);
    uint64_t idx = static_cast<uint64_t>(index);

    auto size = NestedVal::getChildrenSize(v);
    if (idx >= size) {
        return nullptr;
    }

    auto val = NestedVal::getChildVal(v, idx);

    jobject element = createJavaObject(env, val, "com/kuzudb/KuzuValue", "v_ref");
    jclass clazz = env->GetObjectClass(element);
    jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
    env->SetBooleanField(element, fieldID, static_cast<jboolean>(true));
    return element;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1get_1data_1type(
    JNIEnv* env, jclass, jobject thisValue) {
    Value* v = getValue(env, thisValue);
    auto* dt = new LogicalType(*v->getDataType());
    return createJavaObject(env, dt, "com/kuzudb/KuzuDataType", "dt_ref");
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1get_1value(
    JNIEnv* env, jclass, jobject thisValue) {
    Value* v = getValue(env, thisValue);
    auto dt = v->getDataType();
    auto logicalTypeId = dt->getLogicalTypeID();

    switch (logicalTypeId) {
    case LogicalTypeID::BOOL: {
        jclass retClass = env->FindClass("java/lang/Boolean");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(Z)V");
        jboolean val = static_cast<jboolean>(v->getValue<bool>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::INT64:
    case LogicalTypeID::SERIAL: {
        jclass retClass = env->FindClass("java/lang/Long");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(J)V");
        jlong val = static_cast<jlong>(v->getValue<int64_t>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::INT32: {
        jclass retClass = env->FindClass("java/lang/Integer");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(I)V");
        jint val = static_cast<jint>(v->getValue<int32_t>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::INT16: {
        jclass retClass = env->FindClass("java/lang/Short");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(S)V");
        jshort val = static_cast<jshort>(v->getValue<int16_t>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::INT8: {
        jclass retClass = env->FindClass("java/lang/Byte");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(B)V");
        jbyte val = static_cast<jbyte>(v->getValue<int8_t>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::UINT32: {
        jclass retClass = env->FindClass("java/lang/Long");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(J)V");
        jlong val = static_cast<jlong>(v->getValue<uint32_t>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::UINT16: {
        jclass retClass = env->FindClass("java/lang/Integer");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(I)V");
        jint val = static_cast<jint>(v->getValue<uint16_t>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::UINT8: {
        jclass retClass = env->FindClass("java/lang/Short");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(S)V");
        jshort val = static_cast<jshort>(v->getValue<uint8_t>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::INT128: {
        jclass bigIntegerClass = env->FindClass("java/math/BigInteger");
        jmethodID ctor = env->GetMethodID(bigIntegerClass, "<init>", "(Ljava/lang/String;)V");
        int128_t int128_val = v->getValue<int128_t>();
        jstring val = env->NewStringUTF(Int128_t::ToString(int128_val).c_str());
        jobject ret = env->NewObject(bigIntegerClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::DOUBLE: {
        jclass retClass = env->FindClass("java/lang/Double");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(D)V");
        jdouble val = static_cast<jdouble>(v->getValue<double>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::FLOAT: {
        jclass retClass = env->FindClass("java/lang/Float");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(F)V");
        jfloat val = static_cast<jfloat>(v->getValue<float>());
        jobject ret = env->NewObject(retClass, ctor, val);
        return ret;
    }
    case LogicalTypeID::DATE: {
        date_t date = v->getValue<date_t>();
        jclass ldClass = env->FindClass("java/time/LocalDate");
        jmethodID ofEpochDay =
            env->GetStaticMethodID(ldClass, "ofEpochDay", "(J)Ljava/time/LocalDate;");
        jobject ret =
            env->CallStaticObjectMethod(ldClass, ofEpochDay, static_cast<jlong>(date.days));
        return ret;
    }
    case LogicalTypeID::TIMESTAMP: {
        timestamp_t ts = v->getValue<timestamp_t>();
        int64_t seconds = ts.value / 1000000L;
        int64_t nano = ts.value % 1000000L * 1000L;
        jclass retClass = env->FindClass("java/time/Instant");
        jmethodID ofEpochSecond =
            env->GetStaticMethodID(retClass, "ofEpochSecond", "(JJ)Ljava/time/Instant;");
        jobject ret = env->CallStaticObjectMethod(retClass, ofEpochSecond, seconds, nano);
        return ret;
    }
    case LogicalTypeID::INTERVAL: {
        jclass retClass = env->FindClass("java/time/Duration");
        jmethodID ofMillis =
            env->GetStaticMethodID(retClass, "ofMillis", "(J)Ljava/time/Duration;");
        interval_t in = v->getValue<interval_t>();
        long millis = Interval::getMicro(in) / 1000;
        jobject ret = env->CallStaticObjectMethod(retClass, ofMillis, millis);
        return ret;
    }
    case LogicalTypeID::INTERNAL_ID: {
        jclass retClass = env->FindClass("com/kuzudb/KuzuInternalID");
        jmethodID ctor = env->GetMethodID(retClass, "<init>", "(JJ)V");
        internalID_t iid = v->getValue<internalID_t>();
        jobject ret = env->NewObject(retClass, ctor, iid.tableID, iid.offset);
        return ret;
    }
    case LogicalTypeID::STRING: {
        std::string str = v->getValue<std::string>();
        jstring ret = env->NewStringUTF(str.c_str());
        return ret;
    }
    case LogicalTypeID::BLOB: {
        auto str = v->getValue<std::string>();
        auto byteBuffer = str.c_str();
        auto ret = env->NewByteArray(str.size());
        env->SetByteArrayRegion(ret, 0, str.size(), (jbyte*)byteBuffer);
        return ret;
    }
    default: {
        // Throw exception here?
        return nullptr;
    }
    }
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1to_1string(
    JNIEnv* env, jclass, jobject thisValue) {
    Value* v = getValue(env, thisValue);
    std::string result_string = v->toString();
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1node_1val_1get_1id(
    JNIEnv* env, jclass, jobject thisNV) {
    auto nv = getValue(env, thisNV);
    auto idVal = NodeVal::getNodeIDVal(nv);
    if (idVal == nullptr) {
        return NULL;
    }
    auto id = idVal->getValue<internalID_t>();
    jclass retClass = env->FindClass("com/kuzudb/KuzuInternalID");
    jmethodID ctor = env->GetMethodID(retClass, "<init>", "(JJ)V");
    jobject ret = env->NewObject(retClass, ctor, id.tableID, id.offset);
    return ret;
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1node_1val_1get_1label_1name(
    JNIEnv* env, jclass, jobject thisNV) {
    auto* nv = getValue(env, thisNV);
    auto labelVal = NodeVal::getLabelVal(nv);
    if (labelVal == nullptr) {
        return NULL;
    }
    auto label = labelVal->getValue<std::string>();
    return env->NewStringUTF(label.c_str());
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1node_1val_1get_1property_1size(
    JNIEnv* env, jclass, jobject thisNV) {
    auto* nv = getValue(env, thisNV);
    auto size = NodeVal::getNumProperties(nv);
    return static_cast<jlong>(size);
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1node_1val_1get_1property_1name_1at(
    JNIEnv* env, jclass, jobject thisNV, jlong index) {
    auto* nv = getValue(env, thisNV);
    auto propertyName = NodeVal::getPropertyName(nv, index);
    return env->NewStringUTF(propertyName.c_str());
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1node_1val_1get_1property_1value_1at(
    JNIEnv* env, jclass, jobject thisNV, jlong index) {
    auto* nv = getValue(env, thisNV);
    auto propertyValue = NodeVal::getPropertyVal(nv, index);
    jobject ret = createJavaObject(env, propertyValue, "com/kuzudb/KuzuValue", "v_ref");
    jclass clazz = env->GetObjectClass(ret);
    jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
    env->SetBooleanField(ret, fieldID, static_cast<jboolean>(true));
    return ret;
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1node_1val_1to_1string(
    JNIEnv* env, jclass, jobject thisNV) {
    auto* nv = getValue(env, thisNV);
    std::string result_string = NodeVal::toString(nv);
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1rel_1val_1get_1src_1id(
    JNIEnv* env, jclass, jobject thisRV) {
    auto* rv = getValue(env, thisRV);
    auto srcIdVal = RelVal::getSrcNodeIDVal(rv);
    if (srcIdVal == nullptr) {
        return NULL;
    }
    internalID_t id = srcIdVal->getValue<internalID_t>();
    jclass retClass = env->FindClass("com/kuzudb/KuzuInternalID");
    jmethodID ctor = env->GetMethodID(retClass, "<init>", "(JJ)V");
    jobject ret = env->NewObject(retClass, ctor, id.tableID, id.offset);
    return ret;
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1rel_1val_1get_1dst_1id(
    JNIEnv* env, jclass, jobject thisRV) {
    auto* rv = getValue(env, thisRV);
    auto dstIdVal = RelVal::getDstNodeIDVal(rv);
    if (dstIdVal == nullptr) {
        return NULL;
    }
    internalID_t id = dstIdVal->getValue<internalID_t>();
    jclass retClass = env->FindClass("com/kuzudb/KuzuInternalID");
    jmethodID ctor = env->GetMethodID(retClass, "<init>", "(JJ)V");
    jobject ret = env->NewObject(retClass, ctor, id.tableID, id.offset);
    return ret;
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1rel_1val_1get_1label_1name(
    JNIEnv* env, jclass, jobject thisRV) {
    auto* rv = getValue(env, thisRV);
    auto labelVal = RelVal::getLabelVal(rv);
    if (labelVal == nullptr) {
        return NULL;
    }
    auto label = labelVal->getValue<std::string>();
    return env->NewStringUTF(label.c_str());
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1rel_1val_1get_1property_1size(
    JNIEnv* env, jclass, jobject thisRV) {
    auto* rv = getValue(env, thisRV);
    auto size = RelVal::getNumProperties(rv);
    return static_cast<jlong>(size);
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1rel_1val_1get_1property_1name_1at(
    JNIEnv* env, jclass, jobject thisRV, jlong index) {
    auto* rv = getValue(env, thisRV);
    auto name = RelVal::getPropertyName(rv, index);
    return env->NewStringUTF(name.c_str());
}

JNIEXPORT jobject JNICALL Java_com_kuzudb_KuzuNative_kuzu_1rel_1val_1get_1property_1value_1at(
    JNIEnv* env, jclass, jobject thisRV, jlong index) {
    auto* rv = getValue(env, thisRV);
    uint64_t idx = static_cast<uint64_t>(index);
    Value* val = RelVal::getPropertyVal(rv, idx);

    jobject ret = createJavaObject(env, val, "com/kuzudb/KuzuValue", "v_ref");
    jclass clazz = env->GetObjectClass(ret);
    jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
    env->SetBooleanField(ret, fieldID, static_cast<jboolean>(true));
    return ret;
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1rel_1val_1to_1string(
    JNIEnv* env, jclass, jobject thisRV) {
    auto* rv = getValue(env, thisRV);
    std::string result_string = RelVal::toString(rv);
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

JNIEXPORT jstring JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1get_1struct_1field_1name(
    JNIEnv* env, jclass, jobject thisSV, jlong index) {
    auto* sv = getValue(env, thisSV);
    auto dataType = sv->getDataType();
    auto fieldNames = StructType::getFieldNames(dataType);
    if (index >= fieldNames.size() || index < 0) {
        return nullptr;
    }
    auto name = fieldNames[index];
    return env->NewStringUTF(name.c_str());
}

JNIEXPORT jlong JNICALL Java_com_kuzudb_KuzuNative_kuzu_1value_1get_1struct_1index(
    JNIEnv* env, jclass, jobject thisSV, jstring field_name) {
    auto* sv = getValue(env, thisSV);
    const char* field_name_cstr = env->GetStringUTFChars(field_name, JNI_FALSE);
    auto dataType = sv->getDataType();
    auto index = StructType::getFieldIdx(dataType, field_name_cstr);
    env->ReleaseStringUTFChars(field_name, field_name_cstr);
    if (index == INVALID_STRUCT_FIELD_IDX) {
        return -1;
    } else {
        return static_cast<jlong>(index);
    }
}
