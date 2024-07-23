#include "main/storage_driver.h"

#include "main/client_context.h"
#include "storage/storage_manager.h"
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

// TODO(Guodong/Xiyang): FIX-ME. This code path now has become hard to maintain. I don't think we
// should maintain a so special code path for PyG. Instead, we should find a more general solution
// going through our Scan pipeline. Let's reivist and rework this.
void StorageDriver::scan(const std::string&, const std::string&, offset_t*, size_t, uint8_t*,
    size_t) {
    throw RuntimeException("StorageDriver scan is disabled for now.");
    // Resolve files to read from
    // clientContext->query("BEGIN TRANSACTION READ ONLY;");
    // auto nodeTableID = database->catalog->getTableID(clientContext->getTx(), nodeName);
    // auto propertyID = database->catalog->getTableCatalogEntry(clientContext->getTx(),
    // nodeTableID)
    // ->getPropertyID(propertyName);
    // auto nodeTable =
    // ku_dynamic_cast<Table*, NodeTable*>(database->storageManager->getTable(nodeTableID));
    // auto& column = nodeTable->getColumn(propertyID);
    // auto current_buffer = result;
    // std::vector<std::thread> threads;
    // auto numElementsPerThread = size / numThreads + 1;
    // auto sizeLeft = size;
    // auto dummyReadOnlyTransaction = Transaction::getDummyReadOnlyTrx();
    // while (sizeLeft > 0) {
    // uint64_t sizeToRead = std::min(numElementsPerThread, sizeLeft);
    // threads.emplace_back(&StorageDriver::scanColumn, this, dummyReadOnlyTransaction.get(),
    // &column, offsets, sizeToRead, current_buffer);
    // offsets += sizeToRead;
    // current_buffer += sizeToRead * getDataTypeSizeInChunk(column.getDataType());
    // sizeLeft -= sizeToRead;
    // }
    // for (auto& thread : threads) {
    // thread.join();
    // }
    // clientContext->query("COMMIT");
}

uint64_t StorageDriver::getNumNodes(const std::string& nodeName) {
    clientContext->query("BEGIN TRANSACTION READ ONLY;");
    auto nodeTableID = database->catalog->getTableID(clientContext->getTx(), nodeName);
    auto numRows = database->storageManager->getTable(nodeTableID)->getNumRows();
    clientContext->query("COMMIT");
    return numRows;
}

uint64_t StorageDriver::getNumRels(const std::string& relName) {
    clientContext->query("BEGIN TRANSACTION READ ONLY;");
    auto relTableID = database->catalog->getTableID(clientContext->getTx(), relName);
    auto numRows = database->storageManager->getTable(relTableID)->getNumRows();
    clientContext->query("COMMIT");
    return numRows;
}

void StorageDriver::scanColumn(Transaction*, Column*, offset_t*, size_t, uint8_t*) {
    // const auto& dataType = column->getDataType();
    // if (dataType.getPhysicalType() == PhysicalTypeID::LIST ||
    // dataType.getPhysicalType() == PhysicalTypeID::ARRAY) {
    // auto resultVector = ValueVector(dataType.copy());
    // for (auto i = 0u; i < size; ++i) {
    // auto nodeOffset = offsets[i];
    // auto [nodeGroupIdx, offsetInChunk] =
    // StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    // ChunkState readState;
    // column->initChunkState(transaction, nodeGroupIdx, readState);
    // column->scan(transaction, readState, offsetInChunk, offsetInChunk + 1, &resultVector,
    // i);
}
// auto dataVector = ListVector::getDataVector(&resultVector);
// auto dataVectorSize = ListVector::getDataVectorSize(&resultVector);
// auto dataChildTypeSize = LogicalTypeUtils::getRowLayoutSize(dataVector->dataType);
// memcpy(result, dataVector->getData(), dataVectorSize * dataChildTypeSize);
// } else {
// column->batchLookup(transaction, offsets, size, result);
// }

} // namespace main
} // namespace kuzu
