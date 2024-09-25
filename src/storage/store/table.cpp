#include "storage/store/table.h"

#include "common/serializer/deserializer.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void TableScanState::resetOutVectors() {
    for (const auto& outputVector : outputVectors) {
        KU_ASSERT(outputVector->state.get() == outState);
        KU_UNUSED(outputVector);
        outputVector->resetAuxiliaryBuffer();
    }
    outState->getSelVectorUnsafe().setToUnfiltered();
}

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
    auto tableType = TableType::UNKNOWN;
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

bool Table::scan(transaction::Transaction* transaction, TableScanState& scanState) {
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
