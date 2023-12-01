#include "c_api/kuzu.h"
#include "common/exception/exception.h"
#include "main/kuzu.h"

using namespace kuzu::main;
using namespace kuzu::common;

kuzu_database* kuzu_database_init(const char* database_path, kuzu_system_config config) {
    auto database = (kuzu_database*)malloc(sizeof(kuzu_database));
    std::string database_path_str = database_path;
    try {
        database->_database = new Database(
            database_path_str, SystemConfig(config.buffer_pool_size, config.max_num_threads,
                                   config.enable_compression, config.read_only));
    } catch (Exception& e) {
        free(database);
        return nullptr;
    }
    return database;
}

void kuzu_database_destroy(kuzu_database* database) {
    if (database == nullptr) {
        return;
    }
    if (database->_database != nullptr) {
        delete static_cast<Database*>(database->_database);
    }
    free(database);
}

void kuzu_database_set_logging_level(const char* logging_level) {
    Database::setLoggingLevel(logging_level);
}

kuzu_system_config kuzu_default_system_config() {
    return {
        0 /*bufferPoolSize*/, 0 /*maxNumThreads*/, true /*enableCompression*/, false /*readOnly*/};
}
