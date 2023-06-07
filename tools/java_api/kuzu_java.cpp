#include <iostream>
#include <unordered_map>
#include "KuzuNative.h"
#include "main/kuzu.h"
#include "binder/bound_statement_result.h"
#include "planner/logical_plan/logical_plan.h"
#include "common/exception.h"
#include "common/types/value.h"
#include "main/query_summary.h"
#include "json.hpp"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;


jobject createJavaObject (JNIEnv * env, void * memAddress, std::string classPath, std::string refFieldName) {
    uint64_t address = reinterpret_cast<uint64_t>(memAddress);
    jlong ref = static_cast<jlong>(address);

    jclass javaClass = env->FindClass(classPath.c_str());
    jobject newObject = env->AllocObject(javaClass);
    jfieldID refID = env->GetFieldID(javaClass , refFieldName.c_str(), "J");
    env->SetLongField(newObject, refID, ref);
    return newObject;
}

Database* getDatabase (JNIEnv * env, jobject thisDB) {
    jclass javaDBClass = env->GetObjectClass(thisDB);
    jfieldID fieldID = env->GetFieldID(javaDBClass, "db_ref", "J");
    jlong fieldValue = env->GetLongField(thisDB, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Database* db = reinterpret_cast<Database*>(address);
    return db;
}

Connection* getConnection (JNIEnv * env, jobject thisConn) {
    jclass javaConnClass = env->GetObjectClass(thisConn);
    jfieldID fieldID = env->GetFieldID(javaConnClass, "conn_ref", "J");
    jlong fieldValue = env->GetLongField(thisConn, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Connection* conn = reinterpret_cast<Connection*>(address);
    return conn;
}

PreparedStatement* getPreparedStatement(JNIEnv * env, jobject thisPS) {
    jclass javaPSClass = env->GetObjectClass(thisPS);
    jfieldID fieldID = env->GetFieldID(javaPSClass, "ps_ref", "J");
    jlong fieldValue = env->GetLongField(thisPS, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    PreparedStatement* ps = reinterpret_cast<PreparedStatement*>(address);
    return ps;
}


QueryResult* getQueryResult(JNIEnv * env, jobject thisQR) {
    jclass javaQRClass = env->GetObjectClass(thisQR);
    jfieldID fieldID = env->GetFieldID(javaQRClass, "qr_ref", "J");
    jlong fieldValue = env->GetLongField(thisQR, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    QueryResult* qr = reinterpret_cast<QueryResult*>(address);
    return qr;
}


FlatTuple* getFlatTuple(JNIEnv * env, jobject thisFT) {
    jclass javaFTClass = env->GetObjectClass(thisFT);
    jfieldID fieldID = env->GetFieldID(javaFTClass, "ft_ref", "J");
    jlong fieldValue = env->GetLongField(thisFT, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    FlatTuple* ft = reinterpret_cast<FlatTuple*>(address);
    return ft;
}

DataType* getDataType (JNIEnv * env, jobject thisDT) {
    jclass javaDTClass = env->GetObjectClass(thisDT);
    jfieldID fieldID = env->GetFieldID(javaDTClass, "dt_ref", "J");
    jlong fieldValue = env->GetLongField(thisDT, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    DataType* dt = reinterpret_cast<DataType*>(address);
    return dt;
}

Value* getValue (JNIEnv * env, jobject thisValue) {
    jclass javaValueClass = env->GetObjectClass(thisValue);
    jfieldID fieldID = env->GetFieldID(javaValueClass, "v_ref", "J");
    jlong fieldValue = env->GetLongField(thisValue, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Value* v = reinterpret_cast<Value*>(address);
    return v;
}

NodeVal* getNodeVal (JNIEnv * env, jobject thisNodeVal) {
    jclass javaValueClass = env->GetObjectClass(thisNodeVal);
    jfieldID fieldID = env->GetFieldID(javaValueClass, "nv_ref", "J");
    jlong fieldValue = env->GetLongField(thisNodeVal, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    NodeVal* nv = reinterpret_cast<NodeVal*>(address);
    return nv;
}

RelVal* getRelVal (JNIEnv * env, jobject thisRelVal) {
    jclass javaValueClass = env->GetObjectClass(thisRelVal);
    jfieldID fieldID = env->GetFieldID(javaValueClass, "rv_ref", "J");
    jlong fieldValue = env->GetLongField(thisRelVal, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    RelVal* rv = reinterpret_cast<RelVal*>(address);
    return rv;
}

QuerySummary* getQuerySummary (JNIEnv * env, jobject thisQR) {
    jclass javaQRClass = env->GetObjectClass(thisQR);
    jfieldID fieldID = env->GetFieldID(javaQRClass, "qr_ref", "J");
    jlong fieldValue = env->GetLongField(thisQR, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    QuerySummary* qr = reinterpret_cast<QuerySummary*>(address);
    return qr;
}

internalID_t getInternalID (JNIEnv * env, jobject id) {
	jclass javaIDClass = env->GetObjectClass(id);
	jfieldID fieldID = env->GetFieldID(javaIDClass, "table_id", "J");
	long table_id = static_cast<long>(env->GetLongField(id, fieldID));
	fieldID = env->GetFieldID(javaIDClass, "offset", "J");
	long offset = static_cast<long>(env->GetLongField(id, fieldID));
	return internalID_t(offset, table_id);
}

std::string dataTypeIDToString (uint8_t id) {
    switch (id)
    {
        case DataTypeID::ANY: return "ANY";
        case DataTypeID::NODE: return "NODE";
        case DataTypeID::REL: return "REL";
        case DataTypeID::SERIAL: return "SERIAL";
        case DataTypeID::BOOL: return "BOOL";
        case DataTypeID::INT64: return "INT64";
        case DataTypeID::INT32: return "INT32";
        case DataTypeID::INT16: return "INT16";
        case DataTypeID::DOUBLE: return "DOUBLE";
        case DataTypeID::FLOAT: return "FLOAT";
        case DataTypeID::DATE: return "DATE";
        case DataTypeID::TIMESTAMP: return "TIMESTAMP";
        case DataTypeID::INTERVAL: return "INTERVAL";
        case DataTypeID::FIXED_LIST: return "FIXED_LIST";
        case DataTypeID::INTERNAL_ID: return "INTERNAL_ID";
        case DataTypeID::STRING: return "STRING";
        case DataTypeID::VAR_LIST: return "VAR_LIST";
        case DataTypeID::STRUCT: return "STRUCT";
        default: throw std::invalid_argument("Unimplemented item");
    }
}

void javaMapToCPPMap (JNIEnv * env, jobject javaMap, std::unordered_map<std::string, std::shared_ptr<Value>>& cppMap) {

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

    while (env->CallBooleanMethod(iter, hasNext)) {
        jobject entry = env->CallObjectMethod(iter, next);
        jstring key = (jstring) env->CallObjectMethod(entry, entryGetKey);
        jobject value = env->CallObjectMethod(entry, entryGetValue);
        const char* keyStr = env->GetStringUTFChars(key, NULL);
        Value* v = getValue(env, value);
        auto value_ptr = std::make_shared<Value>(v);

        cppMap.insert({keyStr, value_ptr});

        env->DeleteLocalRef(entry);
        env->ReleaseStringUTFChars(key, keyStr);
        env->DeleteLocalRef(key);
        env->DeleteLocalRef(value);
  }
}

/**
 * All Database native functions
*/

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1database_1init 
    (JNIEnv * env, jclass, jstring database_path, jlong buffer_pool_size) {

    const char* path = env->GetStringUTFChars(database_path, JNI_FALSE);
    uint64_t buffer = static_cast<uint64_t>(buffer_pool_size);

	Database* db = buffer == 0 ? new Database(path) :
								 new Database(path, SystemConfig(buffer));
	uint64_t address = reinterpret_cast<uint64_t>(db);

	env->ReleaseStringUTFChars(database_path, path);
	return static_cast<jlong>(address);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1database_1destroy
  (JNIEnv * env, jclass, jobject thisDB) {
    Database* db = getDatabase(env, thisDB);
    delete db;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1database_1set_1logging_1level
  (JNIEnv * env, jclass, jstring logging_level, jobject thisDB) {
    Database* db = getDatabase(env, thisDB);
    const char * lvl = env->GetStringUTFChars(logging_level, JNI_FALSE);
    db->setLoggingLevel(lvl);
	env->ReleaseStringUTFChars(logging_level, lvl);
}

/**
 * All Connection native functions
*/

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1init
  (JNIEnv *env , jclass, jobject db) {
    Database* conn_db = getDatabase(env, db);

    Connection* conn = new Connection(conn_db);
    uint64_t connAddress = reinterpret_cast<uint64_t>(conn);

    return static_cast<jlong>(connAddress);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1destroy
  (JNIEnv * env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    delete conn;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1begin_1read_1only_1transaction
  (JNIEnv * env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->beginReadOnlyTransaction();
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1begin_1write_1transaction
  (JNIEnv * env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->beginWriteTransaction();
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1commit
  (JNIEnv * env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->commit();
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1rollback
  (JNIEnv * env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->rollback();
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1set_1max_1num_1thread_1for_1exec
  (JNIEnv * env, jclass, jobject thisConn, jlong num_threads) {
    Connection* conn = getConnection(env, thisConn);
    uint64_t threads = static_cast<uint64_t>(num_threads);
    conn->setMaxNumThreadForExec(threads);
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1get_1max_1num_1thread_1for_1exec
  (JNIEnv * env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    uint64_t threads = conn->getMaxNumThreadForExec();
    jlong num_threads = static_cast<jlong>(threads);
    return num_threads;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1query
  (JNIEnv * env, jclass, jobject thisConn, jstring query) {
    Connection* conn = getConnection(env, thisConn);
    const char * CPPQuery = env->GetStringUTFChars(query, JNI_FALSE);
    auto query_result = conn->query(CPPQuery);
	env->ReleaseStringUTFChars(query, CPPQuery);

    uint64_t qrAddress = reinterpret_cast<uint64_t>(query_result.get());
    query_result.release();
    jlong qr_ref = static_cast<jlong>(qrAddress);

    jclass qrClass = env->FindClass("tools/java_api/KuzuQueryResult");
    jobject newQRObject = env->AllocObject(qrClass);
    jfieldID refID = env->GetFieldID(qrClass , "qr_ref", "J");
    env->SetLongField(newQRObject, refID, qr_ref);
    return newQRObject;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1prepare
  (JNIEnv * env, jclass, jobject thisConn, jstring query) {
    Connection* conn = getConnection(env, thisConn);
    const char * cppquery = env->GetStringUTFChars(query, JNI_FALSE);

    PreparedStatement* prepared_statement = conn->prepare(cppquery).release();
	env->ReleaseStringUTFChars(query, cppquery);
    if (prepared_statement == nullptr) {
        return nullptr;
    }

    jobject ret = createJavaObject(env, prepared_statement, "tools/java_api/KuzuPreparedStatement", "ps_ref");
    return ret;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1execute
  (JNIEnv * env, jclass, jobject thisConn, jobject preStm, jobject param_map) {
    Connection* conn = getConnection(env, thisConn);
    PreparedStatement* ps = getPreparedStatement(env, preStm);

    std::unordered_map<std::string, std::shared_ptr<Value>> param;
    javaMapToCPPMap(env, param_map, param);

    auto query_result = conn->executeWithParams(ps, param).release();
    if (query_result == nullptr) {
        return nullptr;
    }

    jobject ret = createJavaObject(env, query_result, "tools/java_api/KuzuQueryResult", "qr_ref");
    return ret;
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1get_1node_1table_1names
  (JNIEnv * env , jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    jstring result = env->NewStringUTF(conn->getNodeTableNames().c_str());
    return result;
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1get_1rel_1table_1names
  (JNIEnv * env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    jstring result = env->NewStringUTF(conn->getRelTableNames().c_str());
    return result;
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1get_1node_1property_1names
  (JNIEnv * env, jclass, jobject thisConn, jstring table_name) {
    Connection* conn = getConnection(env, thisConn);
    const char * name = env->GetStringUTFChars(table_name, JNI_FALSE);
    jstring result = env->NewStringUTF(conn->getNodePropertyNames(name).c_str());
	env->ReleaseStringUTFChars(table_name, name);
    return result;
}   

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1get_1rel_1property_1names
  (JNIEnv * env, jclass, jobject thisConn, jstring table_name) {
    Connection* conn = getConnection(env, thisConn);
    const char * name = env->GetStringUTFChars(table_name, JNI_FALSE);
    jstring result = env->NewStringUTF(conn->getRelPropertyNames(name).c_str());
	env->ReleaseStringUTFChars(table_name, name);
    return result;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1interrupt
  (JNIEnv * env, jclass, jobject thisConn) {
    Connection* conn = getConnection(env, thisConn);
    conn->interrupt();
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1set_1query_1timeout
  (JNIEnv * env, jclass, jobject thisConn, jlong timeout_in_ms) {
    Connection* conn = getConnection(env, thisConn);
    uint64_t timeout = static_cast<uint64_t>(timeout_in_ms);
    conn->setQueryTimeOut(timeout);
}

/**
 * All PreparedStatement native functions
*/

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1destroy
  (JNIEnv * env, jclass, jobject thisPS) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    delete ps;
}

JNIEXPORT jboolean JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1allow_1active_1transaction
  (JNIEnv * env, jclass, jobject thisPS) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    return static_cast<jboolean>(ps->allowActiveTransaction());
}

JNIEXPORT jboolean JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1is_1success
  (JNIEnv * env, jclass, jobject thisPS) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    return static_cast<jboolean>(ps->isSuccess());
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1get_1error_1message
  (JNIEnv * env, jclass, jobject thisPS) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string errorMessage = ps->getErrorMessage();
    jstring msg = env->NewStringUTF(errorMessage.c_str());
    return msg;
}

/*
JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1bool
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jboolean value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    bool cppvalue = static_cast<bool>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1int64
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jlong value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    int64_t cppvalue = static_cast<int64_t>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1int32
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jint value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    int32_t cppvalue = static_cast<int32_t>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1int16
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jshort value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    int16_t cppvalue = static_cast<int16_t>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1double
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jdouble value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    double cppvalue = static_cast<double>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1float
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jfloat value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    float cppvalue = static_cast<float>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1date
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jobject value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);

    jclass javaDateClass = env->GetObjectClass(value);
    jfieldID fieldID = env->GetFieldID(javaDateClass, "days", "I");
    jint days = env->GetLongField(value, fieldID);

    int cppdays = static_cast<int>(days);
    auto value_ptr = std::make_shared<Value>(date_t(cppdays));
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1timestamp
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jobject value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);

    jclass javaTimestampClass = env->GetObjectClass(value);
    jfieldID fieldID = env->GetFieldID(javaTimestampClass, "value", "J");
    jint time = env->GetLongField(value, fieldID);

    int64_t cpptime = static_cast<int>(time);
    auto value_ptr = std::make_shared<Value>(timestamp_t(cpptime));
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1interval
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jobject value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);

    jclass javaIntervalClass = env->GetObjectClass(value);
    jfieldID monthsFieldID = env->GetFieldID(javaIntervalClass, "month", "I");
    jfieldID daysFieldID = env->GetFieldID(javaIntervalClass, "days", "I");
    jfieldID microsFieldID = env->GetFieldID(javaIntervalClass, "micros", "J");

    jint months = env->GetLongField(value, monthsFieldID);
    jint days = env->GetLongField(value, daysFieldID);
    jint micros = env->GetLongField(value, microsFieldID);

    int32_t cppMonths = static_cast<int32_t>(months);
    int32_t cppDays = static_cast<int32_t>(days);
    int64_t cppMicros = static_cast<int64_t>(micros);

    auto value_ptr = std::make_shared<Value>(interval_t(cppMonths, cppDays, cppMicros));
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1string
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jstring value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    std::string cppValue = env->GetStringUTFChars(value, JNI_FALSE);
    
    auto value_ptr = std::make_shared<Value>(cppValue);
    ps->bound_values.insert({name, value_ptr});
}


JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1value
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jobject value) {
    PreparedStatement* ps = getPreparedStatement(env, thisPS);
    Value* v = getValue(env, value);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);

    auto value_ptr = std::make_shared<Value>(v);
    ps->bound_values.insert({name, value_ptr});
}

*/

/**
 * All QueryResult native functions
*/

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1destroy
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    delete qr;
}

JNIEXPORT jboolean JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1is_1success
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    return static_cast<jboolean>(qr->isSuccess());
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1get_1error_1message
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    std::string errorMessage = qr->getErrorMessage();
    jstring msg = env->NewStringUTF(errorMessage.c_str());
    return msg;
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1get_1num_1columns
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    return static_cast<jlong>(qr->getNumColumns());
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1get_1column_1name
  (JNIEnv * env, jclass, jobject thisQR, jlong index) {
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

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1get_1column_1data_1type
  (JNIEnv * env, jclass, jobject thisQR, jlong index) {
    QueryResult* qr = getQueryResult(env, thisQR);
    auto column_datatypes = qr->getColumnDataTypes();
    uint64_t idx = static_cast<uint64_t>(index);
    if (idx >= column_datatypes.size()) {
        return nullptr;
    }
    DataType column_datatype = column_datatypes[idx];
    DataType* cdt_copy = new DataType(column_datatype);

    uint64_t dtAddress = reinterpret_cast<uint64_t>(cdt_copy);
    jlong dt_ref = static_cast<jlong>(dtAddress);

    jclass dtClass = env->FindClass("tools/java_api/KuzuDataType");
    jobject newDTObject = env->AllocObject(dtClass);
    jfieldID refID = env->GetFieldID(dtClass , "dt_ref", "J");
    env->SetLongField(newDTObject, refID, dt_ref);
    return newDTObject;
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1get_1num_1tuples
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    return static_cast<jlong>(qr->getNumTuples());
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1get_1query_1summary
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    auto query_summary = qr->getQuerySummary();

    uint64_t qsAddress = reinterpret_cast<uint64_t>(query_summary);
    jlong qs_ref = static_cast<jlong>(qsAddress);

    jclass qsClass = env->FindClass("tools/java_api/KuzuQuerySummary");
    jobject newQSObject = env->AllocObject(qsClass);
    jfieldID refID = env->GetFieldID(qsClass , "qs_ref", "J");
    env->SetLongField(newQSObject, refID, qs_ref);
    return newQSObject;
}

JNIEXPORT jboolean JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1has_1next
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    return static_cast<jboolean>(qr->hasNext());
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1get_1next
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    auto flat_tuple = qr->getNext();

    uint64_t ftAddress = reinterpret_cast<uint64_t>(flat_tuple.get());
    jlong ft_ref = static_cast<jlong>(ftAddress);

    jclass ftClass = env->FindClass("tools/java_api/KuzuFlatTuple");
    jobject newFTObject = env->AllocObject(ftClass);
    jfieldID refID = env->GetFieldID(ftClass , "ft_ref", "J");
    env->SetLongField(newFTObject, refID, ft_ref);
    return newFTObject;
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1to_1string
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    std::string result_string = qr->toString();
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1write_1to_1csv
  (JNIEnv * env, jclass, jobject thisQR, jstring file_path, jchar delimiter, jchar escape_char, jchar new_line) {
    QueryResult* qr = getQueryResult(env, thisQR);
    const char * cpp_file_path = env->GetStringUTFChars(file_path, JNI_FALSE);

    // TODO: confirm this convertion is ok to do. 
    // jchar is 16-bit unicode character so converting to char will lose the higher oreder-bits
    char cpp_delimiter = static_cast<char>(delimiter);
    char cpp_escape_char = static_cast<char>(escape_char);
    char cpp_new_line = static_cast<char>(new_line);

    qr->writeToCSV(cpp_file_path, cpp_delimiter, cpp_escape_char, cpp_new_line);
	env->ReleaseStringUTFChars(file_path, cpp_file_path);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1reset_1iterator
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    qr->resetIterator();
}

/**
 * All FlatTuple native functions
*/

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1flat_1tuple_1destroy
  (JNIEnv * env, jclass, jobject thisFT) {
    FlatTuple* ft = getFlatTuple(env, thisFT);
    delete ft;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1flat_1tuple_1get_1value
  (JNIEnv * env, jclass, jobject thisFT, jlong index) {
    FlatTuple* ft = getFlatTuple(env, thisFT);
    uint32_t idx = static_cast<uint32_t>(index);
    Value* value = ft->getValue(idx);

    return createJavaObject(env, value, "tools/java_api/KuzuValue", "v_ref");
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1flat_1tuple_1to_1string
  (JNIEnv * env, jclass, jobject thisFT) {
    FlatTuple* ft = getFlatTuple(env, thisFT);
    std::string result_string = ft->toString();
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

/**
 * All DataType native functions
*/

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1data_1type_1create
  (JNIEnv * env, jclass, jobject id, jobject child_type, jlong fixed_num_elements_in_list) {
    jclass javaIDClass = env->GetObjectClass(id);
    jfieldID fieldID = env->GetFieldID(javaIDClass, "value", "I");
    jint fieldValue = env->GetIntField(id, fieldID);

    uint8_t data_type_id_u8 = static_cast<uint8_t>(fieldValue);
    DataType* data_type;
    if (child_type == nullptr) {
        data_type = new DataType(static_cast<DataTypeID>(data_type_id_u8));
    } else {
        auto child_type_pty = std::make_unique<DataType>(*getDataType(env, child_type));
        uint64_t num = static_cast<uint64_t>(fixed_num_elements_in_list);
        data_type = num > 0 ?
                        new DataType(std::move(child_type_pty), num) :
                        new DataType(std::move(child_type_pty));
    }
    uint64_t address = reinterpret_cast<uint64_t>(data_type);
    return static_cast<jlong>(address);
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1data_1type_1clone
  (JNIEnv * env, jclass, jobject thisDT) {
    DataType* oldDT = getDataType(env, thisDT);
    DataType* newDT = new DataType(*oldDT);

    jobject dt = createJavaObject(env, newDT, "tools/java_api/KuzuDataType", "dt_ref");
    return dt;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1data_1type_1destroy
  (JNIEnv * env, jclass, jobject thisDT) {
    DataType* dt = getDataType(env, thisDT);
    delete dt;
}

JNIEXPORT jboolean JNICALL Java_tools_java_1api_KuzuNative_kuzu_1data_1type_1equals
  (JNIEnv * env, jclass, jobject dt1, jobject dt2) {
    DataType* cppdt1 = getDataType(env, dt1);
    DataType* cppdt2 = getDataType(env, dt2);

    return static_cast<jboolean>(*cppdt1 == *cppdt2);
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1data_1type_1get_1id
  (JNIEnv * env, jclass, jobject thisDT) {

    DataType* dt = getDataType(env, thisDT);
    uint8_t id_u8 = static_cast<uint8_t>(dt->getTypeID());
    std::string id_str = dataTypeIDToString(id_u8);
    jclass idClass = env->FindClass("tools/java_api/KuzuDataTypeID");
    jfieldID idField = env->GetStaticFieldID(idClass, id_str.c_str(), "Ltools/java_api/KuzuDataTypeID;");
    std::cout << idField << std::endl;
    jobject id = env->GetStaticObjectField(idClass, idField);
    return id;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1data_1type_1get_1child_1type
  (JNIEnv * env, jclass, jobject thisDT) {
    DataType* parent_type = getDataType(env, thisDT);
    if (parent_type->getTypeID() != DataTypeID::FIXED_LIST &&
        parent_type->getTypeID() != DataTypeID::VAR_LIST) {
        return nullptr;
    }
    DataType* child_type = parent_type->getChildType();
    if (child_type == nullptr) {
        return nullptr;
    }

    DataType* new_child_type = new DataType(*child_type);
    jobject ret = createJavaObject(env, new_child_type, "tools/java_api/KuzuDataType", "dt_ref");
    return ret;
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1data_1type_1get_1fixed_1num_1elements_1in_1list
  (JNIEnv * env, jclass, jobject thisDT) {
    DataType* dt = getDataType(env, thisDT);
    if (dt->getTypeID() != DataTypeID::FIXED_LIST) {
        return 0;
    }
    auto extra_info = dt->getExtraTypeInfo();
    if (extra_info == nullptr) {
        return 0;
    }
    auto fixed_list_info = dynamic_cast<FixedListTypeInfo*>(extra_info);
    return static_cast<jlong>(fixed_list_info->getFixedNumElementsInList());
}

/**
 * All Value native functions
*/

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1create_1null
  (JNIEnv * env, jclass) {
    Value* v = new Value(Value::createNullValue());
    jobject ret = createJavaObject(env, v, "tools/java_api/KuzuValue", "v_ref");
    return ret;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1create_1null_1with_1data_1type
  (JNIEnv * env, jclass, jobject data_type) {
    DataType* dt = getDataType(env, data_type);
    Value* v = new Value(Value::createNullValue(*dt));
    jobject ret = createJavaObject(env, v, "tools/java_api/KuzuValue", "v_ref");
    return ret;
}

JNIEXPORT jboolean JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1is_1null
  (JNIEnv * env, jclass, jobject thisV) {
    Value* v = getValue(env, thisV);
    return static_cast<jboolean>(v->isNull());
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1set_1null
  (JNIEnv * env, jclass, jobject thisV, jboolean is_null) {
    Value* v = getValue(env, thisV);
    v->setNull(static_cast<bool>(is_null));
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1create_1default
  (JNIEnv * env, jclass, jobject data_type) {
    DataType* dt = getDataType(env, data_type);
    Value* v = new Value(Value::createDefaultValue(*dt));
    jobject ret = createJavaObject(env, v, "tools/java_api/KuzuValue", "v_ref");
    return ret;
}


JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1create_1value
  (JNIEnv * env, jclass, jobject val) {
    Value* v;
    jclass val_class = env->GetObjectClass(val);
    if (env->IsInstanceOf(val, env->FindClass("java/lang/Boolean"))) {
        jboolean value = env->CallIntMethod(val, env->GetMethodID(val_class, "booleanValue", "()Z"));
        v = new Value(static_cast<bool>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Short"))) {
        jshort value = env->CallIntMethod(val, env->GetMethodID(val_class, "shortValue", "()S"));
        v = new Value(static_cast<int16_t>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Integer"))) {
        jint value = env->CallIntMethod(val, env->GetMethodID(val_class, "intValue", "()I"));
        v = new Value(static_cast<int32_t>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Long"))) {
        jlong value = env->CallIntMethod(val, env->GetMethodID(val_class, "longValue", "()J"));
        v = new Value(static_cast<int64_t>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Float"))) {
        jfloat value = env->CallIntMethod(val, env->GetMethodID(val_class, "floatValue", "()F"));
        v = new Value(static_cast<float>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/Double"))) {
        jdouble value = env->CallIntMethod(val, env->GetMethodID(val_class, "doubleValue", "()D"));
        v = new Value(static_cast<double>(value));
    } else if (env->IsInstanceOf(val, env->FindClass("java/lang/String"))) {
        jstring value = static_cast<jstring>(val);
        const char * str = env->GetStringUTFChars(value, NULL);
        v = new Value(str);
		env->ReleaseStringUTFChars(value, str);
    } else if (env->IsInstanceOf(val, env->FindClass("tools/java_api/KuzuInternalID"))) {
        jfieldID fieldID = env->GetFieldID(val_class, "table_id", "J");
		long table_id = static_cast<long>(env->GetLongField(val, fieldID));
		fieldID = env->GetFieldID(val_class, "offset", "J");
		long offset = static_cast<long>(env->GetLongField(val, fieldID));
		internalID_t id(offset, table_id);
		v = new Value(id);
    } else if (env->IsInstanceOf(val, env->FindClass("tools/java_api/KuzuNodeValue"))) {
        auto node_val = std::make_unique<NodeVal>(*getNodeVal(env, val));
		v = new Value(std::move(node_val));
    } else if (env->IsInstanceOf(val, env->FindClass("tools/java_api/KuzuRelValue"))) {
		auto rel_val = std::make_unique<RelVal>(*getRelVal(env, val));
		v = new Value(std::move(rel_val));
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

		long micro = (seconds * 1000000L) +  (nano / 1000L);
		v = new Value(timestamp_t(micro));
    } else if (env->IsInstanceOf(val, env->FindClass("tools/java_api/KuzuInterval"))) {
		jfieldID monthFieldID = env->GetFieldID(val_class, "months", "I");
    	jfieldID dayFieldID = env->GetFieldID(val_class, "days", "I");
		jfieldID microFieldID = env->GetFieldID(val_class, "micros", "J");
		
		jlong months = env->GetLongField(val, monthFieldID);
		jlong days = env->GetLongField(val, dayFieldID);
		jlong micros = env->GetLongField(val, microFieldID);
		v = new Value(interval_t(months, days, micros));
    } else {
		// Throw exception here
		return -1;
	}
	uint64_t address = reinterpret_cast<uint64_t>(v);
    return static_cast<jlong>(address);
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1clone
  (JNIEnv * env, jclass, jobject thisValue) {
	Value* v = getValue(env, thisValue);
	Value* copy = new Value(*v);
	return createJavaObject(env, copy, "tools/java_api/KuzuValue", "v_ref");
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1copy
  (JNIEnv * env, jclass, jobject thisValue, jobject otherValue) {
	Value* thisV = getValue(env, thisValue);
	Value* otherV = getValue(env, otherValue);
	thisV->copyValueFrom(*otherV);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1destroy
  (JNIEnv * env, jclass, jobject thisValue) {
	Value* v = getValue(env, thisValue);
	delete v;
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1get_1list_1size
  (JNIEnv * env, jclass, jobject thisValue) {
	Value* v = getValue(env, thisValue);
	return static_cast<jlong>(v->getListValReference().size());
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1get_1list_1element
  (JNIEnv * env, jclass, jobject thisValue, jlong index) {
	Value* v = getValue(env, thisValue);
	uint64_t idx = static_cast<uint64_t>(index);

	auto& list_val = v->getListValReference();

	if (idx >= list_val.size()) {
        return nullptr;
    }

	auto& list_element = list_val[index];
	auto val = list_element.get();
	
	jobject element = createJavaObject(env, val, "tools/java_api/KuzuValue", "v_ref");
	jclass clazz = env->GetObjectClass(element);
	jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
	env->SetBooleanField(element, fieldID, static_cast<jboolean>(true));
	return element;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1get_1data_1type
  (JNIEnv * env, jclass, jobject thisValue) {
	Value* v = getValue(env, thisValue);
	DataType* dt = new DataType(v->getDataType());
	return createJavaObject(env, dt, "tools/java_api/KuzuDataType", "dt_ref");
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1get_1value
  (JNIEnv * env, jclass, jobject thisValue) {
	Value* v = getValue(env, thisValue);
	DataType dt = v->getDataType();
	
	switch(dt.typeID) {
	case DataTypeID::BOOL:
	{
		jclass retClass = env->FindClass("java/lang/Boolean");
		jmethodID ctor = env->GetMethodID(retClass, "<init>", "(Z)V");
		jboolean val = static_cast<jboolean>(v->getValue<bool>());
		jobject ret = env->NewObject(retClass, ctor, val);
		return ret;
	}
	case DataTypeID::INT64:
	{
		jclass retClass = env->FindClass("java/lang/Long");
		jmethodID ctor = env->GetMethodID(retClass, "<init>", "(J)V");
		jlong val = static_cast<jlong>(v->getValue<int64_t>());
		jobject ret = env->NewObject(retClass, ctor, val);
		return ret;
	}
	case DataTypeID::INT32:
	{
		jclass retClass = env->FindClass("java/lang/Integer");
		jmethodID ctor = env->GetMethodID(retClass, "<init>", "(I)V");
		jint val = static_cast<jint>(v->getValue<int32_t>());
		jobject ret = env->NewObject(retClass, ctor, val);
		return ret;
	}
	case DataTypeID::INT16:
	{
		jclass retClass = env->FindClass("java/lang/Short");
		jmethodID ctor = env->GetMethodID(retClass, "<init>", "(S)V");
		jshort val = static_cast<jshort>(v->getValue<int16_t>());
		jobject ret = env->NewObject(retClass, ctor, val);
		return ret;
	}
	case DataTypeID::DOUBLE:
	{
		jclass retClass = env->FindClass("java/lang/Double");
		jmethodID ctor = env->GetMethodID(retClass, "<init>", "(D)V");
		jdouble val = static_cast<jdouble>(v->getValue<double>());
		jobject ret = env->NewObject(retClass, ctor, val);
		return ret;
	}
	case DataTypeID::FLOAT:
	{
		jclass retClass = env->FindClass("java/lang/Float");
		jmethodID ctor = env->GetMethodID(retClass, "<init>", "(F)V");
		jfloat val = static_cast<jfloat>(v->getValue<float>());
		jobject ret = env->NewObject(retClass, ctor, val);
		return ret;
	}
	case DataTypeID::DATE:
	{
		jclass retClass = env->FindClass("java/time/LocalDate");
		date_t date = v->getValue<date_t>();
		jclass ldClass = env->FindClass("java/time/LocalDate");
		jmethodID ofEpochDay = env->GetStaticMethodID(ldClass, "ofEpochDay", "(J)Ljava/time/LocalDate;");
    	jobject ret = env->CallStaticObjectMethod(ldClass, ofEpochDay, date.days);
		return ret;
	}
	case DataTypeID::TIMESTAMP:
	{
		timestamp_t ts = v->getValue<timestamp_t>();
		int64_t seconds = ts.value / 1000000L;
		int64_t nano = ts.value % 1000000L * 1000L;
		jclass retClass = env->FindClass("java/time/Instant");
	    jmethodID ofEpochSecond = env->GetStaticMethodID(retClass, "ofEpochSecond", "(JJ)Ljava/time/Instant;");
	    jobject ret = env->CallStaticObjectMethod(retClass, ofEpochSecond, seconds, nano);
		return ret;
	}
	case DataTypeID::INTERVAL:
	{
		jclass retClass = env->FindClass("tools/java_api/KuzuInterval");
		jmethodID ctor = env->GetMethodID(retClass, "<init>", "(IIJ)V");
		interval_t in = v->getValue<interval_t>();
		jobject ret = env->NewObject(retClass, ctor, in.months, in.days, in.micros);
		return ret;
	}
	case DataTypeID::INTERNAL_ID:
	{
		jclass retClass = env->FindClass("tools/java_api/KuzuInternalID");
		jmethodID ctor = env->GetMethodID(retClass, "<init>", "(JJ)V");
		internalID_t iid = v->getValue<internalID_t>();
		jobject ret = env->NewObject(retClass, ctor, iid.tableID, iid.offset);
		return ret;
	}
	case DataTypeID::STRING:
	{
		std::string str = v->getValue<std::string>();
		jstring ret = env->NewStringUTF(str.c_str());
		return ret;
	}
	case DataTypeID::NODE:
	{
		auto node_val = v->getValue<NodeVal>();
		NodeVal* nv = new NodeVal(node_val);
		return createJavaObject(env, nv, "tools/java_api/KuzuNodeValue", "nv_ref");
	}
	case DataTypeID::REL:
	{
		auto rel_val = v->getValue<RelVal>();
		RelVal* nv = new RelVal(rel_val);
		return createJavaObject(env, nv, "tools/java_api/KuzuRelValue", "rv_ref");
	}
	default:
	{	
		//Throw exception here?
		return nullptr;
	}

	return nullptr;
	}
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1value_1to_1string
  (JNIEnv * env, jclass, jobject thisValue) {
	Value* v = getValue(env, thisValue);
    std::string result_string = v->toString();
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1create
  (JNIEnv * env, jclass, jobject id, jstring label) {
	jclass idClass = env->FindClass("tools/java_api/KuzuInternalID");
	jfieldID fieldID = env->GetFieldID(idClass, "table_id", "J");
	long table_id = static_cast<long>(env->GetLongField(id, fieldID));
	fieldID = env->GetFieldID(idClass, "offset", "J");
	long offset = static_cast<long>(env->GetLongField(id, fieldID));

	auto id_val = std::make_unique<Value>(internalID_t(offset, table_id));
	const char * labelstr = env->GetStringUTFChars(label, JNI_FALSE);
	auto label_val = std::make_unique<Value>(labelstr);

	NodeVal* node_val = new NodeVal(std::move(id_val), std::move(label_val));
	uint64_t address = reinterpret_cast<uint64_t>(node_val);
	env->ReleaseStringUTFChars(label, labelstr);
    return static_cast<jlong>(address);
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1clone
  (JNIEnv * env, jclass, jobject thisNV) {
	NodeVal* nv = getNodeVal(env, thisNV);
	NodeVal* newNV = new NodeVal(*nv);
	return createJavaObject(env, newNV, "tools/java_api/KuzuNodeValue", "nv_ref");
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1destroy
  (JNIEnv * env, jclass, jobject thisNV) {
	NodeVal* nv = getNodeVal(env, thisNV);
	delete nv;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1get_1id_1val
  (JNIEnv * env, jclass, jobject thisNV) {
	NodeVal* nv = getNodeVal(env, thisNV);
	auto idVal = nv->getNodeIDVal();

	jobject ret = createJavaObject(env, idVal, "tools/java_api/KuzuValue", "v_ref");
	jclass clazz = env->GetObjectClass(ret);
	jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
	env->SetBooleanField(ret, fieldID, static_cast<jboolean>(true));
	return ret;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1get_1label_1val
  (JNIEnv * env, jclass, jobject thisNV) {
	NodeVal* nv = getNodeVal(env, thisNV);
	auto labelVal = nv->getNodeIDVal();

	jobject ret = createJavaObject(env, labelVal, "tools/java_api/KuzuValue", "v_ref");
	jclass clazz = env->GetObjectClass(ret);
	jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
	env->SetBooleanField(ret, fieldID, static_cast<jboolean>(true));
	return ret;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1get_1id
  (JNIEnv * env, jclass, jobject thisNV) {
	NodeVal* nv = getNodeVal(env, thisNV);
	auto id = nv->getNodeID();

	jclass retClass = env->FindClass("tools/java_api/KuzuInternalID");
	jmethodID ctor = env->GetMethodID(retClass, "<init>", "(JJ)V");
	jobject ret = env->NewObject(retClass, ctor, id.tableID, id.offset);
	return ret;
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1get_1label_1name
  (JNIEnv * env, jclass, jobject thisNV) {
	NodeVal* nv = getNodeVal(env, thisNV);
	return env->NewStringUTF(nv->getLabelName().c_str());
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1get_1property_1size
  (JNIEnv * env, jclass, jobject thisNV) {
	NodeVal* nv = getNodeVal(env, thisNV);
	return static_cast<jlong>(nv->getProperties().size());
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1get_1property_1name_1at
  (JNIEnv * env, jclass, jobject thisNV, jlong index) {
	NodeVal* nv = getNodeVal(env, thisNV);
	uint64_t idx = static_cast<uint64_t>(index);
	return env->NewStringUTF(nv->getProperties().at(idx).first.c_str());
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1get_1property_1value_1at
  (JNIEnv * env, jclass, jobject thisNV, jlong index) {
	NodeVal* nv = getNodeVal(env, thisNV);
	uint64_t idx = static_cast<uint64_t>(index);
	Value* val = nv->getProperties().at(idx).second.get();

	jobject ret = createJavaObject(env, val, "tools/java_api/KuzuValue", "v_ref");
	jclass clazz = env->GetObjectClass(ret);
	jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
	env->SetBooleanField(ret, fieldID, static_cast<jboolean>(true));
	return ret;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1add_1property
  (JNIEnv * env, jclass, jobject thisNV, jstring key, jobject value) {
	NodeVal* nv = getNodeVal(env, thisNV);
	const char * k = env->GetStringUTFChars(key, JNI_FALSE);
	auto v = std::make_unique<Value>(*getValue(env, value));
	env->ReleaseStringUTFChars(key, k);
	nv->addProperty(k, std::move(v));
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1node_1val_1to_1string
  (JNIEnv * env, jclass, jobject thisNV) {
	NodeVal* nv = getNodeVal(env, thisNV);
	std::string result_string = nv->toString();
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1create
  (JNIEnv * env, jclass, jobject src_id, jobject dst_id, jstring label) {
	internalID_t cpp_src_id = getInternalID(env, src_id);
	internalID_t cpp_dst_id = getInternalID(env, dst_id);
    auto src_id_val = std::make_unique<Value>(internalID_t(cpp_src_id.offset, cpp_src_id.tableID));
    auto dst_id_val = std::make_unique<Value>(internalID_t(cpp_dst_id.offset, cpp_dst_id.tableID));
    const char * lablestr = env->GetStringUTFChars(label, JNI_FALSE);
	auto label_val = std::make_unique<Value>(lablestr);
    RelVal* rv = new RelVal(std::move(src_id_val), std::move(dst_id_val), std::move(label_val));
	
	uint64_t address = reinterpret_cast<uint64_t>(rv);
	env->ReleaseStringUTFChars(label, lablestr);
    return static_cast<jlong>(address); 
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1clone
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	RelVal* newRV = new RelVal(*rv);
	return createJavaObject(env, newRV, "tools/java_api/KuzuRelValue", "rv_ref");
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1destroy
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	delete rv;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1get_1src_1id_1val
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	auto idVal = rv->getSrcNodeIDVal();

	jobject ret = createJavaObject(env, idVal, "tools/java_api/KuzuValue", "v_ref");
	jclass clazz = env->GetObjectClass(ret);
	jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
	env->SetBooleanField(ret, fieldID, static_cast<jboolean>(true));
	return ret;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1get_1dst_1id_1val
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	auto idVal = rv->getDstNodeIDVal();

	jobject ret = createJavaObject(env, idVal, "tools/java_api/KuzuValue", "v_ref");
	jclass clazz = env->GetObjectClass(ret);
	jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
	env->SetBooleanField(ret, fieldID, static_cast<jboolean>(true));
	return ret;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1get_1src_1id
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	internalID_t id = rv->getSrcNodeID();

	jclass retClass = env->FindClass("tools/java_api/KuzuInternalID");
	jmethodID ctor = env->GetMethodID(retClass, "<init>", "(JJ)V");
	jobject ret = env->NewObject(retClass, ctor, id.tableID, id.offset);
	return ret;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1get_1dst_1id
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	internalID_t id = rv->getDstNodeID();

	jclass retClass = env->FindClass("tools/java_api/KuzuInternalID");
	jmethodID ctor = env->GetMethodID(retClass, "<init>", "(JJ)V");
	jobject ret = env->NewObject(retClass, ctor, id.tableID, id.offset);
	return ret;
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1get_1label_1name
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	return env->NewStringUTF(rv->getLabelName().c_str());
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1get_1property_1size
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	return static_cast<jlong>(rv->getProperties().size());
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1get_1property_1name_1at
  (JNIEnv * env, jclass, jobject thisRV, jlong index) {
	RelVal* rv = getRelVal(env, thisRV);
	auto& name = rv->getProperties().at(index).first;
    return env->NewStringUTF(name.c_str());
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1get_1property_1value_1at
  (JNIEnv * env, jclass, jobject thisRV, jlong index) {
	RelVal* rv = getRelVal(env, thisRV);
	uint64_t idx = static_cast<uint64_t>(index);
	Value* val = rv->getProperties().at(idx).second.get();

	jobject ret = createJavaObject(env, val, "tools/java_api/KuzuValue", "v_ref");
	jclass clazz = env->GetObjectClass(ret);
	jfieldID fieldID = env->GetFieldID(clazz, "isOwnedByCPP", "Z");
	env->SetBooleanField(ret, fieldID, static_cast<jboolean>(true));
	return ret;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1add_1property
  (JNIEnv * env, jclass, jobject thisRV, jstring key, jobject value) {
	RelVal* rv = getRelVal(env, thisRV);
	const char * k = env->GetStringUTFChars(key, JNI_FALSE);
	auto v = std::make_unique<Value>(*getValue(env, value));
	rv->addProperty(k, std::move(v));

	env->ReleaseStringUTFChars(key, k);
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1rel_1val_1to_1string
  (JNIEnv * env, jclass, jobject thisRV) {
	RelVal* rv = getRelVal(env, thisRV);
	std::string result_string = rv->toString();
    jstring ret = env->NewStringUTF(result_string.c_str());
    return ret;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1summary_1destroy
  (JNIEnv * env, jclass, jobject thisQR) {
	QuerySummary* qr = getQuerySummary(env, thisQR);
	delete qr;
}

JNIEXPORT jdouble JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1summary_1get_1compiling_1time
  (JNIEnv * env, jclass, jobject thisQR) {
	QuerySummary* qr = getQuerySummary(env, thisQR);
	return static_cast<jdouble>(qr->getCompilingTime());
}

JNIEXPORT jdouble JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1summary_1get_1execution_1time
  (JNIEnv * env, jclass, jobject thisQR) {
	QuerySummary* qr = getQuerySummary(env, thisQR);
	return static_cast<jdouble>(qr->getExecutionTime());
}
