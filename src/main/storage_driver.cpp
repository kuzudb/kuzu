#include "main/storage_driver.h"

#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace main {

StorageDriver::StorageDriver(Database* database)
    : catalog{database->catalog.get()}, storageManager{database->storageManager.get()} {}

StorageDriver::~StorageDriver() = default;

void StorageDriver::scan(const std::string& nodeName, const std::string& propertyName,
    offset_t* offsets, size_t size, uint8_t* result, size_t numThreads) {
    // Resolve files to read from
    auto catalogContent = catalog->getReadOnlyVersion();
    auto nodeTableID = catalogContent->getTableID(nodeName);
    auto propertyID = catalogContent->getTableSchema(nodeTableID)->getPropertyID(propertyName);
    auto nodeTable = storageManager->getNodesStore().getNodeTable(nodeTableID);
    auto column = nodeTable->getPropertyColumn(propertyID);
    auto current_buffer = result;
    std::vector<std::thread> threads;
    auto numElementsPerThread = size / numThreads + 1;
    auto sizeLeft = size;
    auto dummyReadOnlyTransaction = Transaction::getDummyReadOnlyTrx();
    while (sizeLeft > 0) {
        uint64_t sizeToRead = std::min(numElementsPerThread, sizeLeft);
        threads.emplace_back(&StorageDriver::scanColumn, this, dummyReadOnlyTransaction.get(),
            column, offsets, sizeToRead, current_buffer);
        offsets += sizeToRead;
        current_buffer += sizeToRead * column->getNumBytesPerValue();
        sizeLeft -= sizeToRead;
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

uint64_t StorageDriver::getNumNodes(const std::string& nodeName) {
    auto catalogContent = catalog->getReadOnlyVersion();
    auto nodeTableID = catalogContent->getTableID(nodeName);
    auto nodeStatistics = storageManager->getNodesStore()
                              .getNodesStatisticsAndDeletedIDs()
                              .getNodeStatisticsAndDeletedIDs(nodeTableID);
    return nodeStatistics->getNumTuples();
}

uint64_t StorageDriver::getNumRels(const std::string& relName) {
    auto catalogContent = catalog->getReadOnlyVersion();
    auto relTableID = catalogContent->getTableID(relName);
    auto relStatistics =
        storageManager->getRelsStore().getRelsStatistics().getRelStatistics(relTableID);
    return relStatistics->getNumTuples();
}

void StorageDriver::scanColumn(Transaction* transaction, storage::NodeColumn* column,
    offset_t* offsets, size_t size, uint8_t* result) {
    column->batchLookup(transaction, offsets, size, result);
}

} // namespace main
} // namespace kuzu
