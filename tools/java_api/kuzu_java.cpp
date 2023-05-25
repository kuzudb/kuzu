#include <iostream>
#include <unordered_map>
#include "KuzuNative.h"
#include "main/kuzu.h"

using namespace kuzu::main;
using namespace kuzu::common;

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

    jclass javaDBClass = env->GetObjectClass(thisDB);
    jfieldID fieldID = env->GetFieldID(javaDBClass, "db_ref", "J");
    jlong fieldValue = env->GetLongField(thisDB, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Database* db = reinterpret_cast<Database*>(address);

    free(db);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1database_1set_1logging_1level
  (JNIEnv * env, jclass, jstring logging_level, jobject thisDB) {

    jclass javaDBClass = env->GetObjectClass(thisDB);
    jfieldID fieldID = env->GetFieldID(javaDBClass, "db_ref", "J");
    jlong fieldValue = env->GetLongField(thisDB, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Database* db = reinterpret_cast<Database*>(address);

    std::string lvl = env->GetStringUTFChars(logging_level, JNI_FALSE);
    db->setLoggingLevel(lvl);
    
}


JNIEXPORT jlong JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1init
  (JNIEnv *env , jclass, jobject db) {

    jclass javaDBClass = env->GetObjectClass(db);
    jfieldID fieldID = env->GetFieldID(javaDBClass, "db_ref", "J");
    jlong db_ref = env->GetLongField(db, fieldID);

    uint64_t address = static_cast<uint64_t>(db_ref);
    Database* conn_db = reinterpret_cast<Database*>(address);

    Connection* conn = new Connection(conn_db);
    uint64_t connAddress = reinterpret_cast<uint64_t>(conn);

    return static_cast<jlong>(connAddress);
}

JNIEXPORT void JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1destroy
  (JNIEnv * env, jclass, jobject thisConn) {

    jclass javaConnClass = env->GetObjectClass(thisConn);
    jfieldID fieldID = env->GetFieldID(javaConnClass, "conn_ref", "J");
    jlong fieldValue = env->GetLongField(thisConn, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Connection* conn = reinterpret_cast<Connection*>(address);

    free(conn);
}

Connection* getConnection(JNIEnv * env, jobject thisConn) {
    jclass javaConnClass = env->GetObjectClass(thisConn);
    jfieldID fieldID = env->GetFieldID(javaConnClass, "conn_ref", "J");
    jlong fieldValue = env->GetLongField(thisConn, fieldID);

    uint64_t address = static_cast<uint64_t>(fieldValue);
    Connection* conn = reinterpret_cast<Connection*>(address);
    return conn;
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
  (JNIEnv * env, jclass, jobject, jstring query) {
    // TODO: Implement prepared statement
}

JNIEXPORT jobject JNICALL Java_tools_java_1api_KuzuNative_kuzu_1connection_1execute
  (JNIEnv *, jclass, jobject, jobject) {
    // TODO: Implement prepared statement
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

// TODO: Implement prepared statement


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
    std::cout << "result_string back" << std::endl;
    jstring ret = env->NewStringUTF(result_string.c_str());
    std::cout << "newstringUTF back" << std::endl;
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
