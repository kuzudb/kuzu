#include "c_api/kuzu.h"
#include "common/exception/exception.h"
#include "main/kuzu.h"
using namespace kuzu::main;
using namespace kuzu::common;

kuzu_state kuzu_database_init(const char* database_path, kuzu_system_config config,
    kuzu_database* out_database) {
    try {
        std::string database_path_str = database_path;
        auto systemConfig = SystemConfig(config.buffer_pool_size, config.max_num_threads,
            config.enable_compression, config.read_only, config.max_db_size, config.auto_checkpoint,
            config.checkpoint_threshold);

#if defined(__APPLE__)
        systemConfig.threadQos = config.thread_qos;
#endif
        out_database->_database = new Database(database_path_str, systemConfig);
    } catch (Exception& e) {
        out_database->_database = nullptr;
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
    auto cSystemConfig = kuzu_system_config();
    cSystemConfig.buffer_pool_size = config.bufferPoolSize;
    cSystemConfig.max_num_threads = config.maxNumThreads;
    cSystemConfig.enable_compression = config.enableCompression;
    cSystemConfig.read_only = config.readOnly;
    cSystemConfig.max_db_size = config.maxDBSize;
    cSystemConfig.auto_checkpoint = config.autoCheckpoint;
    cSystemConfig.checkpoint_threshold = config.checkpointThreshold;
#if defined(__APPLE__)
    cSystemConfig.thread_qos = config.threadQos;
#endif
    return cSystemConfig;
}
