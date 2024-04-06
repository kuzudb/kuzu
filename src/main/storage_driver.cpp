#include "main/storage_driver.h"

#include <thread>

#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::storage;

namespace kuzu {
namespace main {

StorageDriver::StorageDriver(Database* database)
    : catalog{database->catalog.get()}, storageManager{database->storageManager.get()} {}

void StorageDriver::scan(const std::string& nodeName, const std::string& propertyName,
    offset_t* offsets, size_t size, uint8_t* result, size_t numThreads) {
    // Resolve files to read from
    auto nodeTableID = catalog->getTableID(&DUMMY_READ_TRANSACTION, nodeName);
    auto propertyID = catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, nodeTableID)
                          ->getPropertyID(propertyName);
    auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(storageManager->getTable(nodeTableID));
    auto column = nodeTable->getColumn(propertyID);
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
        // TODO(Guodong/Xiyang/Chang): StorageDriver should figure numBytesPerValue from logicalType
        // and not rely on Column to provide this information.
        current_buffer += sizeToRead * column->getNumBytesPerValue();
        sizeLeft -= sizeToRead;
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

uint64_t StorageDriver::getNumNodes(const std::string& nodeName) {
    auto nodeTableID = catalog->getTableID(&DUMMY_READ_TRANSACTION, nodeName);
    auto nodeStatistics =
        storageManager->getNodesStatisticsAndDeletedIDs()->getNodeStatisticsAndDeletedIDs(
            &DUMMY_READ_TRANSACTION, nodeTableID);
    return nodeStatistics->getNumTuples();
}

uint64_t StorageDriver::getNumRels(const std::string& relName) {
    auto relTableID = catalog->getTableID(&DUMMY_READ_TRANSACTION, relName);
    auto relStatistics = storageManager->getRelsStatistics()->getRelStatistics(relTableID,
        Transaction::getDummyReadOnlyTrx().get());
    return relStatistics->getNumTuples();
}

void StorageDriver::scanColumn(Transaction* transaction, storage::Column* column, offset_t* offsets,
    size_t size, uint8_t* result) {
    auto dataType = column->getDataType();
    if (dataType.getPhysicalType() == PhysicalTypeID::LIST) {
        auto resultVector = ValueVector(dataType);
        for (auto i = 0u; i < size; ++i) {
            auto nodeOffset = offsets[i];
            auto [nodeGroupIdx, offsetInChunk] =
                StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
            column->scan(transaction, nodeGroupIdx, offsetInChunk, offsetInChunk + 1, &resultVector,
                i);
        }
        auto dataVector = ListVector::getDataVector(&resultVector);
        auto dataVectorSize = ListVector::getDataVectorSize(&resultVector);
        auto dataChildTypeSize = LogicalTypeUtils::getRowLayoutSize(dataVector->dataType);
        memcpy(result, dataVector->getData(), dataVectorSize * dataChildTypeSize);
    } else {
        column->batchLookup(transaction, offsets, size, result);
    }
}

} // namespace main
} // namespace kuzu
