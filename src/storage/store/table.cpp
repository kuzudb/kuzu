#include "storage/store/table.h"

#include "common/serializer/deserializer.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

Table::Table(const catalog::TableCatalogEntry* tableEntry, const StorageManager* storageManager,
    MemoryManager* memoryManager)
    : tableType{tableEntry->getTableType()}, tableID{tableEntry->getTableID()},
      tableName{tableEntry->getName()}, enableCompression{storageManager->compressionEnabled()},
      dataFH{storageManager->getDataFH()}, memoryManager{memoryManager},
      shadowFile{&storageManager->getShadowFile()}, hasChanges{false} {}

std::unique_ptr<Table> Table::loadTable(Deserializer& deSer, const catalog::Catalog& catalog,
    StorageManager* storageManager, MemoryManager* memoryManager, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    std::string key;
    TableType tableType;
    deSer.validateDebuggingInfo(key, "table_type");
    deSer.deserializeValue<TableType>(tableType);
    std::unique_ptr<Table> table;
    switch (tableType) {
    case TableType::NODE: {
        table = NodeTable::loadTable(deSer, catalog, storageManager, memoryManager, vfs, context);
    } break;
    case TableType::REL: {
        table = RelTable::loadTable(deSer, catalog, storageManager, memoryManager, vfs, context);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    table->tableType = tableType;
    return table;
}

static bool skipScan(const transaction::Transaction* transaction, const TableScanState& scanState) {
    return transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
}

bool Table::scan(transaction::Transaction* transaction, TableScanState& scanState) {
    if (skipScan(transaction, scanState) && scanState.source == TableScanSource::NONE) {
        return false;
    }
    for (const auto& outputVector : scanState.outputVectors) {
        KU_ASSERT(outputVector->state.get() == scanState.outState);
        KU_UNUSED(outputVector);
        outputVector->resetAuxiliaryBuffer();
    }
    scanState.outState->getSelVectorUnsafe().setToUnfiltered();
    return scanInternal(transaction, scanState);
}

void Table::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("table_type");
    serializer.write<TableType>(tableType);
    serializer.writeDebuggingInfo("table_id");
    serializer.write<table_id_t>(tableID);
}

std::unique_ptr<DataChunk> Table::constructDataChunk(const std::vector<LogicalType>& types) {
    auto dataChunk = std::make_unique<DataChunk>(types.size());
    for (auto i = 0u; i < types.size(); i++) {
        auto valueVector = std::make_unique<ValueVector>(types[i].copy(), memoryManager);
        dataChunk->insert(i, std::move(valueVector));
    }
    return dataChunk;
}

} // namespace storage
} // namespace kuzu
