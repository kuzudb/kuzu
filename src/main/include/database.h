#pragma once

// TODO: Consider using forward declaration
#include "src/common/include/configs.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/graph.h"
#include "src/storage/include/memory_manager.h"

using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::processor;

namespace graphflow {
namespace main {

struct SystemConfig {

    explicit SystemConfig(
        uint64_t defaultPageBufferPoolSize = common::StorageConfig::DEFAULT_BUFFER_POOL_SIZE,
        uint64_t largePageBufferPoolSize = common::StorageConfig::DEFAULT_BUFFER_POOL_SIZE)
        : defaultPageBufferPoolSize{defaultPageBufferPoolSize}, largePageBufferPoolSize{
                                                                    largePageBufferPoolSize} {}

    uint64_t defaultPageBufferPoolSize;
    uint64_t largePageBufferPoolSize;

    uint64_t maxNumThreads = std::thread::hardware_concurrency();
};

struct DatabaseConfig {

    explicit DatabaseConfig(std::string databasePath, bool inMemoryMode = true)
        : databasePath{std::move(databasePath)}, inMemoryMode{inMemoryMode} {}

    std::string databasePath;
    bool inMemoryMode;
};

class Database {
    friend class Connection;

public:
    explicit Database(const DatabaseConfig& databaseConfig)
        : Database{databaseConfig, SystemConfig()} {}

    explicit Database(const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig);

    ~Database() = default;

    void resizeBufferManager(uint64_t newSize);

    // TODO: interface below might need to be removed eventually
    // used in storage test
    inline storage::Graph* getGraph() { return graph.get(); }
    // used in shell for getting debug info
    inline storage::BufferManager* getBufferManager() { return bufferManager.get(); }
    // used in API test
    inline uint64_t getDefaultBMSize() const { return systemConfig.defaultPageBufferPoolSize; }
    inline uint64_t getLargeBMSize() const { return systemConfig.largePageBufferPoolSize; }

private:
    DatabaseConfig databaseConfig;
    SystemConfig systemConfig;
    std::unique_ptr<common::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<storage::Graph> graph;
};

} // namespace main
} // namespace graphflow
