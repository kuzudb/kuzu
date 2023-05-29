#include <iostream>
#include <unordered_map>
#include "KuzuNative.h"
#include "main/kuzu.h"
#include "binder/bound_statement_result.h"
#include "planner/logical_plan/logical_plan.h"
#include "common/exception.h"
#include "common/types/value.h"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;

class PreStmtWithBound {
    public:
    PreparedStatement* preStatement;
    std::unordered_map<std::string, std::shared_ptr<Value>> bound_values;
};

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

PreStmtWithBound* getPreStmtWithBound(JNIEnv * env, jobject thisPS) {
    jclass javaPSClass = env->GetObjectClass(thisPS);
    jfieldID fieldID = env->GetFieldID(javaPSClass, "ps_ref", "J");
    jlong fieldValue = env->GetLongField(thisPS, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    PreStmtWithBound* ps = reinterpret_cast<PreStmtWithBound*>(address);
    return ps;
}

Value* getValue (JNIEnv * env, jobject thisValue) {
    jclass javaValueClass = env->GetObjectClass(thisValue);
    jfieldID fieldID = env->GetFieldID(javaValueClass, "v_ref", "J");
    jlong fieldValue = env->GetLongField(thisValue, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Value* v = reinterpret_cast<Value*>(address);
    return v;
}

JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1database_1init 
    (JNIEnv * env, jclass, jstring database_path, jlong buffer_pool_size) {

    // TODO: use buffer_pool_size when creating the db
    std::string path = env->GetStringUTFChars(database_path, JNI_FALSE);
    uint64_t buffer = static_cast<uint64_t>(buffer_pool_size);

    Database* db = new Database(path);
    uint64_t address = reinterpret_cast<uint64_t>(db);

    return static_cast<jlong>(address);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1database_1destroy
  (JNIEnv * env, jclass, jobject thisDB) {
    Database* db = getDatabase(env, thisDB);
    free(db);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1database_1set_1logging_1level
  (JNIEnv * env, jclass, jstring logging_level, jobject thisDB) {
    Database* db = getDatabase(env, thisDB);
    std::string lvl = env->GetStringUTFChars(logging_level, JNI_FALSE);
    db->setLoggingLevel(lvl);
}


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
    free(conn);
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
    std::string CPPQuery = env->GetStringUTFChars(query, JNI_FALSE);
    auto query_result = conn->query(CPPQuery);

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
    std::string cppquery = env->GetStringUTFChars(query, JNI_FALSE);

    PreparedStatement* prepared_statement = conn->prepare(cppquery).release();
    if (prepared_statement == nullptr) {
        return nullptr;
    }
    PreStmtWithBound* ps = new PreStmtWithBound();
    ps->preStatement = prepared_statement;
    
    jobject ret = createJavaObject(env, ps, "tools/java_api/KuzuPreparedStatement", "ps_ref");
    return ret;
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1execute
  (JNIEnv * env, jclass, jobject thisConn, jobject preStm) {
    Connection* conn = getConnection(env, thisConn);
    PreStmtWithBound* ps = getPreStmtWithBound(env, preStm);

    auto query_result = conn->executeWithParams(ps->preStatement, ps->bound_values).release();
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
    std::string name = env->GetStringUTFChars(table_name, JNI_FALSE);
    jstring result = env->NewStringUTF(conn->getNodePropertyNames(name).c_str());
    return result;
}   

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1get_1rel_1property_1names
  (JNIEnv * env, jclass, jobject thisConn, jstring table_name) {
    Connection* conn = getConnection(env, thisConn);
    std::string name = env->GetStringUTFChars(table_name, JNI_FALSE);
    jstring result = env->NewStringUTF(conn->getRelPropertyNames(name).c_str());
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

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1destroy
  (JNIEnv * env, jclass, jobject thisPS) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    ps->bound_values.clear();
    free(ps);
}

JNIEXPORT jboolean JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1allow_1active_1transaction
  (JNIEnv * env, jclass, jobject thisPS) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    return static_cast<jboolean>(ps->preStatement->allowActiveTransaction());
}

JNIEXPORT jboolean JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1is_1success
  (JNIEnv * env, jclass, jobject thisPS) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    return static_cast<jboolean>(ps->preStatement->isSuccess());
}

JNIEXPORT jstring JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1get_1error_1message
  (JNIEnv * env, jclass, jobject thisPS) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    std::string errorMessage = ps->preStatement->getErrorMessage();
    jstring msg = env->NewStringUTF(errorMessage.c_str());
    return msg;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1bool
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jboolean value) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    bool cppvalue = static_cast<bool>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1int64
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jlong value) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    int64_t cppvalue = static_cast<int64_t>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1int32
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jint value) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    int32_t cppvalue = static_cast<int32_t>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1int16
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jshort value) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    int16_t cppvalue = static_cast<int16_t>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1double
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jdouble value) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    double cppvalue = static_cast<double>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1float
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jfloat value) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    float cppvalue = static_cast<float>(value);
    auto value_ptr = std::make_shared<Value>(cppvalue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1date
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jobject value) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
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
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
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
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
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
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);
    std::string cppValue = env->GetStringUTFChars(value, JNI_FALSE);
    
    auto value_ptr = std::make_shared<Value>(cppValue);
    ps->bound_values.insert({name, value_ptr});
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1prepared_1statement_1bind_1value
  (JNIEnv * env, jclass, jobject thisPS, jstring param_name, jobject value) {
    PreStmtWithBound* ps = getPreStmtWithBound(env, thisPS);
    Value* v = getValue(env, value);
    std::string name = env->GetStringUTFChars(param_name, JNI_FALSE);

    auto value_ptr = std::make_shared<Value>(v);
    ps->bound_values.insert({name, value_ptr});
}

QueryResult* getQueryResult(JNIEnv * env, jobject thisQR) {
    jclass javaQRClass = env->GetObjectClass(thisQR);
    jfieldID fieldID = env->GetFieldID(javaQRClass, "qr_ref", "J");
    jlong fieldValue = env->GetLongField(thisQR, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    QueryResult* qr = reinterpret_cast<QueryResult*>(address);
    return qr;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1destroy
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    free(qr);
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
    std::string cpp_file_path = env->GetStringUTFChars(file_path, JNI_FALSE);

    // TODO: confirm this convertion is ok to do. 
    // jchar is 16-bit unicode character so converting to char will lose the higher oreder-bits
    char cpp_delimiter = static_cast<char>(delimiter);
    char cpp_escape_char = static_cast<char>(escape_char);
    char cpp_new_line = static_cast<char>(new_line);

    qr->writeToCSV(cpp_file_path, cpp_delimiter, cpp_escape_char, cpp_new_line);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1query_1result_1reset_1iterator
  (JNIEnv * env, jclass, jobject thisQR) {
    QueryResult* qr = getQueryResult(env, thisQR);
    qr->resetIterator();
}

FlatTuple* getFlatTuple(JNIEnv * env, jobject thisFT) {
    jclass javaFTClass = env->GetObjectClass(thisFT);
    jfieldID fieldID = env->GetFieldID(javaFTClass, "ft_ref", "J");
    jlong fieldValue = env->GetLongField(thisFT, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    FlatTuple* ft = reinterpret_cast<FlatTuple*>(address);
    return ft;
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1flat_1tuple_1destroy
  (JNIEnv * env, jclass, jobject thisFT) {
    FlatTuple* ft = getFlatTuple(env, thisFT);
    free(ft);
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
