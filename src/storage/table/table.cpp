#include "storage/table/table.h"

#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

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

Table::Table(const catalog::TableCatalogEntry* tableEntry, const StorageManager* storageManager,
    MemoryManager* memoryManager)
    : tableType{tableEntry->getTableType()}, tableID{tableEntry->getTableID()},
      tableName{tableEntry->getName()}, enableCompression{storageManager->compressionEnabled()},
      dataFH{storageManager->getDataFH()}, memoryManager{memoryManager},
      shadowFile{&storageManager->getShadowFile()}, hasChanges{false},
      inMemory{storageManager->isInMemory()}, readOnly{storageManager->isReadOnly()} {}

bool Table::scan(transaction::Transaction* transaction, TableScanState& scanState) {
    return scanInternal(transaction, scanState);
}

DataChunk Table::constructDataChunk(MemoryManager* mm, std::vector<LogicalType> types) {
    DataChunk dataChunk(types.size());
    for (auto i = 0u; i < types.size(); i++) {
        auto valueVector = std::make_unique<ValueVector>(std::move(types[i]), mm);
        dataChunk.insert(i, std::move(valueVector));
    }
    return dataChunk;
}

} // namespace storage
} // namespace kuzu
