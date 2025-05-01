#include "c_api/kuzu.h"
#include "c_api/helpers.h"
#include "common/exception/exception.h"
#include "main/kuzu.h"
using namespace kuzu::main;
using namespace kuzu::common;

kuzu_state kuzu_database_init(const char* database_path, kuzu_system_config config,
    kuzu_database* out_database, char** error_message) {
    try {
        std::string database_path_str = database_path;
        out_database->_database = new Database(database_path_str,
            SystemConfig(config.buffer_pool_size, config.max_num_threads, config.enable_compression,
                config.read_only, config.max_db_size, config.auto_checkpoint,
                config.checkpoint_threshold));
    } catch (Exception& e) {
        convertToOwnedErrorMessage(e.what(), error_message);
        return KuzuError;
    }
    return KuzuSuccess;
}

void kuzu_database_destroy(kuzu_database* database) {
    if (database == nullptr) {
        return;
    }
    if (database->_database != nullptr) {
        delete static_cast<Database*>(database->_database);
    }
}

kuzu_system_config kuzu_default_system_config() {
    SystemConfig config = SystemConfig();
    return {config.bufferPoolSize, config.maxNumThreads, config.enableCompression, config.readOnly,
        config.maxDBSize, config.autoCheckpoint, config.checkpointThreshold};
}
