#include "storage/table/table.h"

#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

TableScanState::~TableScanState() = default;

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
void TableScanState::resetOutVectors() {
    for (const auto& outputVector : outputVectors) {
        KU_ASSERT(outputVector->state.get() == outState.get());
        KU_UNUSED(outputVector);
        outputVector->resetAuxiliaryBuffer();
    }
    outState->getSelVectorUnsafe().setToUnfiltered();
}

void TableScanState::setToTable(const transaction::Transaction*, Table* table_,
    std::vector<column_id_t> columnIDs_, std::vector<ColumnPredicateSet> columnPredicateSets_,
    RelDataDirection) {
    table = table_;
    columnIDs = std::move(columnIDs_);
    columnPredicateSets = std::move(columnPredicateSets_);
    nodeGroupScanState->chunkStates.resize(columnIDs.size());
}

TableInsertState::TableInsertState(std::vector<ValueVector*> propertyVectors)
    : propertyVectors{std::move(propertyVectors)}, logToWAL{true} {}
TableInsertState::~TableInsertState() = default;
TableUpdateState::TableUpdateState(column_id_t columnID, ValueVector& propertyVector)
    : columnID{columnID}, propertyVector{propertyVector}, logToWAL{true} {}
TableUpdateState::~TableUpdateState() = default;
TableDeleteState::TableDeleteState() : logToWAL{true} {}
TableDeleteState::~TableDeleteState() = default;

Table::Table(const catalog::TableCatalogEntry* tableEntry, const StorageManager* storageManager,
    MemoryManager* memoryManager)
    : tableType{tableEntry->getTableType()}, tableID{tableEntry->getTableID()},
      tableName{tableEntry->getName()}, enableCompression{storageManager->compressionEnabled()},
      dataFH{storageManager->getDataFH()}, memoryManager{memoryManager},
      shadowFile{&storageManager->getShadowFile()}, hasChanges{false},
      inMemory{storageManager->isInMemory()}, readOnly{storageManager->isReadOnly()} {}

Table::~Table() = default;

bool Table::scan(transaction::Transaction* transaction, TableScanState& scanState) {
    return scanInternal(transaction, scanState);
}

static DataChunk constructBaseDataChunk(const std::vector<LogicalType>& types,
    std::shared_ptr<common::DataChunkState> state) {
    if (state) {
        return DataChunk(types.size(), state);
    } else {
        return DataChunk(types.size());
    }
}

DataChunk Table::constructDataChunk(MemoryManager* mm, std::vector<LogicalType> types,
    std::shared_ptr<common::DataChunkState> state) {
    DataChunk dataChunk = constructBaseDataChunk(types, std::move(state));
    for (auto i = 0u; i < types.size(); i++) {
        auto valueVector = std::make_unique<ValueVector>(std::move(types[i]), mm);
        dataChunk.insert(i, std::move(valueVector));
    }
    return dataChunk;
}

} // namespace storage
} // namespace kuzu
