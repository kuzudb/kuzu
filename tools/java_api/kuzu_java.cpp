#include <iostream>
#include <unordered_map>
#include "KuzuNative.h"
#include "main/kuzu.h"

using namespace kuzu::main;

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
