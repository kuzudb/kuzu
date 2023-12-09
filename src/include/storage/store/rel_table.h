#pragma once

#include "catalog/rel_table_schema.h"
#include "common/column_data_format.h"
#include "storage/stats/rel_table_statistics.h"
#include "storage/store/rel_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace storage {

struct RelDetachDeleteState {
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<common::ValueVector> relIDVector;

    explicit RelDetachDeleteState();
};

class RelTable final : public Table {
public:
    RelTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, RelsStoreStats* relsStoreStats,
        MemoryManager* memoryManager, catalog::RelTableSchema* schema, WAL* wal,
        bool enableCompression);

    inline void initializeReadState(transaction::Transaction* transaction,
        common::RelDataDirection direction, const std::vector<common::column_id_t>& columnIDs,
        common::ValueVector* inNodeIDVector, RelDataReadState* readState) {
        return direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->initializeReadState(
                       transaction, columnIDs, inNodeIDVector, readState) :
                   bwdRelTableData->initializeReadState(
                       transaction, columnIDs, inNodeIDVector, readState);
    }
    void read(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    void insert(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector, common::ValueVector* propertyVector);
    void delete_(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* relIDVector);
    void detachDelete(transaction::Transaction* transaction, common::RelDataDirection direction,
        common::ValueVector* srcNodeIDVector, RelDetachDeleteState* deleteState);
    inline bool checkIfNodeHasRels(transaction::Transaction* transaction,
        common::RelDataDirection direction, common::ValueVector* srcNodeIDVector) {
        return direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->checkIfNodeHasRels(transaction, srcNodeIDVector) :
                   bwdRelTableData->checkIfNodeHasRels(transaction, srcNodeIDVector);
    }

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) override;
    inline void dropColumn(common::column_id_t columnID) override {
        fwdRelTableData->dropColumn(columnID);
        bwdRelTableData->dropColumn(columnID);
    }
    inline Column* getAdjColumn(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getAdjColumn() :
                                                            bwdRelTableData->getAdjColumn();
    }
    inline common::column_id_t getNumColumns() {
        KU_ASSERT(fwdRelTableData->getNumColumns() == bwdRelTableData->getNumColumns());
        return fwdRelTableData->getNumColumns();
    }
    inline Column* getColumn(common::column_id_t columnID, common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getColumn(columnID) :
                                                            bwdRelTableData->getColumn(columnID);
    }

    // This is to make sure for X_TO_ONE table, the adj column is aligned with its src node table in
    // terms of num of node groups, and be correctly filled with initialized null values.
    void resizeColumns(transaction::Transaction* transaction, common::RelDataDirection direction,
        common::node_group_idx_t numNodeGroups);

    inline common::ColumnDataFormat getTableDataFormat(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getDataFormat() :
                                                            bwdRelTableData->getDataFormat();
    }
    inline void append(NodeGroup* nodeGroup, common::RelDataDirection direction) {
        direction == common::RelDataDirection::FWD ? fwdRelTableData->append(nodeGroup) :
                                                     bwdRelTableData->append(nodeGroup);
    }

    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void prepareRollback(LocalTableData* localTable) override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

private:
    void scan(transaction::Transaction* transaction, RelDataReadState& scanState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(transaction::Transaction* transaction, RelDataReadState& scanState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

    common::row_idx_t detachDeleteForRegularRels(transaction::Transaction* transaction,
        RelTableData* tableData, RelTableData* reverseTableData,
        common::ValueVector* srcNodeIDVector, RelDataReadState* relDataReadState,
        RelDetachDeleteState* deleteState);
    common::row_idx_t detachDeleteForCSRRels(transaction::Transaction* transaction,
        RelTableData* tableData, RelTableData* reverseTableData,
        common::ValueVector* srcNodeIDVector, RelDataReadState* relDataReadState,
        RelDetachDeleteState* deleteState);

    inline RelTableData* getDirectedTableData(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData.get() :
                                                            bwdRelTableData.get();
    }

private:
    std::unique_ptr<RelTableData> fwdRelTableData;
    std::unique_ptr<RelTableData> bwdRelTableData;
};

} // namespace storage
} // namespace kuzu
