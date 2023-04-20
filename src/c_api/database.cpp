#include "c_api/kuzu.h"
#include "common/exception.h"
#include "main/kuzu.h"

using namespace kuzu::main;
using namespace kuzu::common;

kuzu_database* kuzu_database_init(const char* database_path, uint64_t buffer_pool_size) {
    auto database = (kuzu_database*)malloc(sizeof(kuzu_database));
    std::string database_path_str = database_path;
    try {
        database->_database = buffer_pool_size == 0 ?
                                  new Database(database_path_str) :
                                  new Database(database_path_str, SystemConfig(buffer_pool_size));
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
