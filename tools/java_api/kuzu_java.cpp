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
