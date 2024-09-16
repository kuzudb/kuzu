#include "main/storage_driver.h"

#include <thread>

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::storage;
using namespace kuzu::catalog;

namespace kuzu {
namespace main {

StorageDriver::StorageDriver(Database* database) : database{database} {
    clientContext = std::make_unique<ClientContext>(database);
}

StorageDriver::~StorageDriver() = default;

static Table* getTable(const ClientContext& context, const std::string& tableName) {
    auto tableID = context.getCatalog()->getTableID(context.getTx(), tableName);
    return context.getStorageManager()->getTable(tableID);
}

static TableCatalogEntry* getEntry(const ClientContext& context, const std::string& tableName) {
    auto catalog = context.getCatalog();
    auto transaction = context.getTx();
    auto tableID = catalog->getTableID(transaction, tableName);
    return catalog->getTableCatalogEntry(transaction, tableID);
}

static bool validateNumericalType(const LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
    case LogicalTypeID::INT128:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
        return true;
    default:
        return false;
    }
}

static std::string getUnsupportedTypeErrMsg(const LogicalType& type) {
    return stringFormat("Unsupported data type {}.", type.toString());
}

static uint32_t getElementSize(const LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
    case LogicalTypeID::INT128:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
        return PhysicalTypeUtils::getFixedTypeSize(type.getPhysicalType());
    case LogicalTypeID::ARRAY: {
        auto& childType = ArrayType::getChildType(type);
        if (!validateNumericalType(childType)) {
            throw RuntimeException(getUnsupportedTypeErrMsg(type));
        }
        auto numElements = ArrayType::getNumElements(type);
        return numElements * PhysicalTypeUtils::getFixedTypeSize(childType.getPhysicalType());
    }
    default:
        throw RuntimeException(getUnsupportedTypeErrMsg(type));
    }
}

void StorageDriver::scan(const std::string& nodeName, const std::string& propertyName,
    common::offset_t* offsets, size_t numOffsets, uint8_t* result, size_t numThreads) {
    clientContext->query("BEGIN TRANSACTION READ ONLY;");
    auto entry = getEntry(*clientContext, nodeName);
    auto columnID = entry->getColumnID(propertyName);
    auto table = getTable(*clientContext, nodeName);
    auto& dataType = table->ptrCast<NodeTable>()->getColumn(columnID).getDataType();
    auto elementSize = getElementSize(dataType);
    auto numOffsetsPerThread = numOffsets / numThreads + 1;
    auto remainingNumOffsets = numOffsets;
    auto current_buffer = result;
    std::vector<std::thread> threads;
    while (remainingNumOffsets > 0) {
        auto numOffsetsToScan = std::min(numOffsetsPerThread, remainingNumOffsets);
        threads.emplace_back(&StorageDriver::scanColumn, this, table, columnID, offsets,
            numOffsetsToScan, current_buffer);
        offsets += numOffsetsToScan;
        current_buffer += numOffsetsToScan * elementSize;
        remainingNumOffsets -= numOffsetsToScan;
    }
    for (auto& thread : threads) {
        thread.join();
    }
    clientContext->query("COMMIT");
}

uint64_t StorageDriver::getNumNodes(const std::string& nodeName) {
    clientContext->query("BEGIN TRANSACTION READ ONLY;");
    auto table = getTable(*clientContext, nodeName);
    clientContext->query("COMMIT");
    return table->getNumRows();
}

uint64_t StorageDriver::getNumRels(const std::string& relName) {
    clientContext->query("BEGIN TRANSACTION READ ONLY;");
    auto table = getTable(*clientContext, relName);
    clientContext->query("COMMIT");
    return table->getNumRows();
}

void StorageDriver::scanColumn(storage::Table* table, column_id_t columnID, offset_t* offsets,
    size_t size, uint8_t* result) {
    // Create scan state.
    auto columnIDs = std::vector<column_id_t>{columnID};
    auto nodeTable = table->ptrCast<NodeTable>();
    auto column = &nodeTable->getColumn(columnID);
    std::vector<Column*> columns;
    columns.push_back(column);
    std::vector<ColumnPredicateSet> emptyPredicateSets;
    auto scanState =
        std::make_unique<NodeTableScanState>(columnIDs, columns, std::move(emptyPredicateSets));
    // Create value vectors
    auto idVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    auto columnVector = std::make_unique<ValueVector>(column->getDataType().copy(),
        clientContext->getMemoryManager());
    auto vectorState = DataChunkState::getSingleValueDataChunkState();
    idVector->state = vectorState;
    columnVector->state = vectorState;
    scanState->rowIdxVector->state = vectorState;
    scanState->nodeIDVector = idVector.get();
    scanState->outputVectors.push_back(columnVector.get());
    // Scan
    // TODO: validate not more than 1 level nested
    auto physicalType = column->getDataType().getPhysicalType();
    switch (physicalType) {
    case PhysicalTypeID::BOOL:
    case PhysicalTypeID::INT128:
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::DOUBLE:
    case PhysicalTypeID::FLOAT: {
        for (auto i = 0u; i < size; ++i) {
            idVector->setValue(0, nodeID_t{offsets[i], table->getTableID()});
            nodeTable->lookup(clientContext->getTx(), *scanState);
            memcpy(result, columnVector->getData(),
                PhysicalTypeUtils::getFixedTypeSize(physicalType));
        }
    } break;
    case PhysicalTypeID::ARRAY: {
        auto& childType = ArrayType::getChildType(column->getDataType());
        auto elementSize = PhysicalTypeUtils::getFixedTypeSize(childType.getPhysicalType());
        auto numElements = ArrayType::getNumElements(column->getDataType());
        auto arraySize = elementSize * numElements;
        for (auto i = 0u; i < size; ++i) {
            idVector->setValue(0, nodeID_t{offsets[i], table->getTableID()});
            nodeTable->lookup(clientContext->getTx(), *scanState);
            auto dataVector = ListVector::getDataVector(columnVector.get());
            memcpy(result, dataVector->getData() + i * arraySize, arraySize);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace main
} // namespace kuzu
