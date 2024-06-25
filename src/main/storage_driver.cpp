#include "main/storage_driver.h"

#include <thread>

#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::storage;

namespace kuzu {
namespace main {

StorageDriver::StorageDriver(Database* database) : database{database} {
    clientContext = std::make_unique<ClientContext>(database);
}

StorageDriver::~StorageDriver() = default;

void StorageDriver::scan(const std::string& nodeName, const std::string& propertyName,
    offset_t* offsets, size_t size, uint8_t* result, size_t numThreads) {
    // Resolve files to read from
    clientContext->query("BEGIN TRANSACTION READ ONLY;");
    auto nodeTableID = database->catalog->getTableID(clientContext->getTx(), nodeName);
    auto propertyID = database->catalog->getTableCatalogEntry(clientContext->getTx(), nodeTableID)
                          ->getPropertyID(propertyName);
    auto nodeTable =
        ku_dynamic_cast<Table*, NodeTable*>(database->storageManager->getTable(nodeTableID));
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
        current_buffer += sizeToRead * storage::getDataTypeSizeInChunk(column->getDataType());
        sizeLeft -= sizeToRead;
    }
    for (auto& thread : threads) {
        thread.join();
    }
    clientContext->query("COMMIT");
}

uint64_t StorageDriver::getNumNodes(const std::string& nodeName) {
    clientContext->query("BEGIN TRANSACTION READ ONLY;");
    auto nodeTableID = database->catalog->getTableID(clientContext->getTx(), nodeName);
    auto nodeStatistics =
        database->storageManager->getNodesStatisticsAndDeletedIDs()->getNodeStatisticsAndDeletedIDs(
            clientContext->getTx(), nodeTableID);
    auto numNodes = nodeStatistics->getNumTuples();
    clientContext->query("COMMIT");
    return numNodes;
}

uint64_t StorageDriver::getNumRels(const std::string& relName) {
    clientContext->query("BEGIN TRANSACTION READ ONLY;");
    auto relTableID = database->catalog->getTableID(clientContext->getTx(), relName);
    auto relStatistics = database->storageManager->getRelsStatistics()->getRelStatistics(relTableID,
        clientContext->getTx());
    auto numRels = relStatistics->getNumTuples();
    clientContext->query("COMMIT");
    return numRels;
}

void StorageDriver::scanColumn(Transaction* transaction, storage::Column* column, offset_t* offsets,
    size_t size, uint8_t* result) {
    const auto& dataType = column->getDataType();
    if (dataType.getPhysicalType() == PhysicalTypeID::LIST ||
        dataType.getPhysicalType() == PhysicalTypeID::ARRAY) {
        auto resultVector = ValueVector(dataType.copy());
        for (auto i = 0u; i < size; ++i) {
            auto nodeOffset = offsets[i];
            auto [nodeGroupIdx, offsetInChunk] =
                StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
            Column::ChunkState readState;
            column->initChunkState(transaction, nodeGroupIdx, readState);
            column->scan(transaction, readState, offsetInChunk, offsetInChunk + 1, &resultVector,
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
