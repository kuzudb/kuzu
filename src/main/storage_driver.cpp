#include "main/storage_driver.h"

#include "catalog/catalog.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

// ThreadPool::ThreadPool(size_t numThreads) {
//    for (auto i = 0u; i < numThreads; ++i) {
//        threads.emplace_back([this] {
//            while (true) {
//                std::function<void()> task;
//                {
//                    std::unique_lock<std::mutex> lck(queueMtx);
//                    condition.wait(lck, [this] { return stop || !tasks.empty(); });
//                    if (stop && tasks.empty()) {
//                        return;
//                    }
//                    task = std::move(tasks.front());
//                    tasks.pop();
//                }
//                task();
//            }
//        });
//    }
//}
//
// ThreadPool::~ThreadPool() {
//    {
//        std::unique_lock<std::mutex> lck(queueMtx);
//        stop = true;
//    }
//    condition.notify_all();
//    for (auto& thread : threads) {
//        thread.join();
//    }
//}

StorageDriver::StorageDriver(kuzu::main::Database* database, size_t numThreads)
    : catalog{database->catalog.get()}, storageManager{database->storageManager.get()},
      numThreads{numThreads} {}

StorageDriver::~StorageDriver() = default;

std::pair<std::unique_ptr<uint8_t[]>, size_t> StorageDriver::scan(const std::string& nodeName,
    const std::string& propertyName, common::offset_t* offsets, size_t size) {
    // Resolve files to read from
    auto catalogContent = catalog->getReadOnlyVersion();
    auto nodeTableID = catalogContent->getTableID(nodeName);
    auto propertyID = catalogContent->getTableSchema(nodeTableID)->getPropertyID(propertyName);
    auto nodeTable = storageManager->getNodesStore().getNodeTable(nodeTableID);
    auto column = nodeTable->getPropertyColumn(propertyID);

    auto bufferSize = column->elementSize * size;
    auto result = std::make_unique<uint8_t[]>(bufferSize);
    auto buffer = result.get();
    std::vector<std::thread> threads;
    auto numElementsPerThread = size / numThreads + 1;
    auto sizeLeft = size;
    while (sizeLeft > 0) {
        auto sizeToRead = std::min(numElementsPerThread, sizeLeft);
        threads.emplace_back(&StorageDriver::scanColumn, this, column, offsets, sizeToRead, buffer);
        offsets += sizeToRead;
        buffer += sizeToRead * column->elementSize;
        sizeLeft -= sizeToRead;
    }
    for (auto& thread : threads) {
        thread.join();
    }
    return std::make_pair(std::move(result), bufferSize);
}

void StorageDriver::scanColumn(
    storage::Column* column, common::offset_t* offsets, size_t size, uint8_t* result) {
    column->scan(offsets, size, result);
}

} // namespace main
} // namespace kuzu
