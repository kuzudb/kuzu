#pragma once

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "storage/store/rel_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace storage {

struct RelTableReadState : public TableReadState {
    common::RelDataDirection direction;

    RelTableReadState(const std::vector<common::column_id_t>& columnIDs,
        common::RelDataDirection direction)
        : TableReadState{columnIDs}, direction{direction} {
        dataReadState = std::make_unique<RelDataReadState>();
    }

    bool hasMoreToRead(transaction::Transaction* transaction) const {
        auto relDataReadState =
            common::ku_dynamic_cast<TableDataReadState*, RelDataReadState*>(dataReadState.get());
        return relDataReadState->hasMoreToRead(transaction);
    }
};

struct RelTableInsertState : public TableInsertState {
    const common::ValueVector& srcNodeIDVector;
    const common::ValueVector& dstNodeIDVector;

    RelTableInsertState(const common::ValueVector& srcNodeIDVector,
        const common::ValueVector& dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors)
        : TableInsertState{std::move(propertyVectors)}, srcNodeIDVector{srcNodeIDVector},
          dstNodeIDVector{dstNodeIDVector} {}
};

struct RelTableUpdateState : public TableUpdateState {
    const common::ValueVector& srcNodeIDVector;
    const common::ValueVector& dstNodeIDVector;
    const common::ValueVector& relIDVector;

    RelTableUpdateState(common::column_id_t columnID, const common::ValueVector& srcNodeIDVector,
        const common::ValueVector& dstNodeIDVector, const common::ValueVector& relIDVector,
        const common::ValueVector& propertyVector)
        : TableUpdateState{columnID, propertyVector}, srcNodeIDVector{srcNodeIDVector},
          dstNodeIDVector{dstNodeIDVector}, relIDVector{relIDVector} {}
};

struct RelTableDeleteState : public TableDeleteState {
    const common::ValueVector& srcNodeIDVector;
    const common::ValueVector& dstNodeIDVector;
    const common::ValueVector& relIDVector;

    RelTableDeleteState(const common::ValueVector& srcNodeIDVector,
        const common::ValueVector& dstNodeIDVector, const common::ValueVector& relIDVector)
        : srcNodeIDVector{srcNodeIDVector}, dstNodeIDVector{dstNodeIDVector},
          relIDVector{relIDVector} {}
};

// TODO(Guodong): Should move inside RelTableDeleteState.
struct RelDetachDeleteState {
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<common::ValueVector> relIDVector;

    explicit RelDetachDeleteState();
};

class RelsStoreStats;
class RelTable final : public Table {
public:
    RelTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, RelsStoreStats* relsStoreStats,
        MemoryManager* memoryManager, catalog::RelTableCatalogEntry* relTableEntry, WAL* wal,
        bool enableCompression);

    void initializeReadState(transaction::Transaction* transaction,
        common::RelDataDirection direction, const std::vector<common::column_id_t>& columnIDs,
        RelTableReadState& readState);

    void readInternal(transaction::Transaction* transaction, TableReadState& readState) override;

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    void delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void detachDelete(transaction::Transaction* transaction, common::RelDataDirection direction,
        common::ValueVector* srcNodeIDVector, RelDetachDeleteState* deleteState);
    void checkIfNodeHasRels(transaction::Transaction* transaction,
        common::RelDataDirection direction, common::ValueVector* srcNodeIDVector);

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) override;
    void dropColumn(common::column_id_t columnID) override {
        fwdRelTableData->dropColumn(columnID);
        bwdRelTableData->dropColumn(columnID);
    }
    Column* getCSROffsetColumn(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getCSROffsetColumn() :
                                                            bwdRelTableData->getCSROffsetColumn();
    }
    Column* getCSRLengthColumn(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getCSRLengthColumn() :
                                                            bwdRelTableData->getCSRLengthColumn();
    }
    common::column_id_t getNumColumns() {
        KU_ASSERT(fwdRelTableData->getNumColumns() == bwdRelTableData->getNumColumns());
        return fwdRelTableData->getNumColumns();
    }
    Column* getColumn(common::column_id_t columnID, common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getColumn(columnID) :
                                                            bwdRelTableData->getColumn(columnID);
    }
    const std::vector<std::unique_ptr<Column>>& getColumns(
        common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData->getColumns() :
                                                            bwdRelTableData->getColumns();
    }

    void append(ChunkedNodeGroup* nodeGroup, common::RelDataDirection direction) {
        direction == common::RelDataDirection::FWD ? fwdRelTableData->append(nodeGroup) :
                                                     bwdRelTableData->append(nodeGroup);
    }

    bool isNewNodeGroup(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->isNewNodeGroup(transaction, nodeGroupIdx) :
                   bwdRelTableData->isNewNodeGroup(transaction, nodeGroupIdx);
    }

    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void prepareCommit() override;
    void prepareRollback(LocalTable* localTable) override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

    RelTableData* getDirectedTableData(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData.get() :
                                                            bwdRelTableData.get();
    }

private:
    void scan(transaction::Transaction* transaction, RelTableReadState& scanState);

    common::row_idx_t detachDeleteForCSRRels(transaction::Transaction* transaction,
        RelTableData* tableData, RelTableData* reverseTableData,
        common::ValueVector* srcNodeIDVector, RelTableReadState* relDataReadState,
        RelDetachDeleteState* deleteState);

private:
    std::unique_ptr<RelTableData> fwdRelTableData;
    std::unique_ptr<RelTableData> bwdRelTableData;
};

} // namespace storage
} // namespace kuzu
