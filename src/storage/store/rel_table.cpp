#include "storage/store/rel_table.h"

#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelTable::RelTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, RelsStoreStats* relsStoreStats,
    BufferManager* bufferManager, RelTableSchema* tableSchema, WAL* wal, bool enableCompression)
    : tableID{tableSchema->getTableID()}, relsStoreStats{relsStoreStats}, wal{wal} {
    fwdRelTableData = std::make_unique<RelTableData>(dataFH, metadataFH, bufferManager, wal,
        tableSchema, relsStoreStats, FWD, enableCompression);
    bwdRelTableData = std::make_unique<RelTableData>(dataFH, metadataFH, bufferManager, wal,
        tableSchema, relsStoreStats, BWD, enableCompression);
}

void RelTable::read(Transaction* transaction, RelDataReadState& scanState,
    RelDataDirection direction, ValueVector* inNodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    if (getTableDataFormat(direction) == ColumnDataFormat::REGULAR &&
        !inNodeIDVector->isSequential()) {
        lookup(transaction, direction, inNodeIDVector, columnIDs, outputVectors);
    } else {
        scan(transaction, scanState, direction, inNodeIDVector, columnIDs, outputVectors);
    }
}

void RelTable::scan(Transaction* transaction, RelDataReadState& scanState,
    RelDataDirection direction, ValueVector* inNodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    auto tableData = getDirectedTableData(direction);
    tableData->scan(transaction, scanState, inNodeIDVector, columnIDs, outputVectors);
}

void RelTable::lookup(Transaction* transaction, RelDataDirection direction,
    ValueVector* inNodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    auto tableData = getDirectedTableData(direction);
    tableData->lookup(transaction, inNodeIDVector, columnIDs, outputVectors);
}

void RelTable::addColumn(
    Transaction* transaction, const Property& property, ValueVector* defaultValueVector) {
    relsStoreStats->addMetadataDAHInfo(tableID, *property.getDataType());
    fwdRelTableData->addColumn(transaction, property, defaultValueVector, relsStoreStats);
    bwdRelTableData->addColumn(transaction, property, defaultValueVector, relsStoreStats);
    wal->addToUpdatedNodeTables(tableID);
}

void RelTable::checkpointInMemory() {
    fwdRelTableData->checkpointInMemory();
    bwdRelTableData->checkpointInMemory();
}

void RelTable::rollbackInMemory() {
    fwdRelTableData->rollbackInMemory();
    bwdRelTableData->rollbackInMemory();
}

} // namespace storage
} // namespace kuzu
