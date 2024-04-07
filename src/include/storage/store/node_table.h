#pragma once

#include <utility>

#include "common/assert.h"
#include "common/cast.h"
#include "storage/index/hash_index.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/node_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace storage {

struct NodeTableInsertState : public TableInsertState {
    common::ValueVector& nodeIDVector;
    const common::ValueVector& pkVector;

    NodeTableInsertState(common::ValueVector& nodeIDVector, const common::ValueVector& pkVector,
        const std::vector<common::ValueVector*>& propertyVectors)
        : TableInsertState{std::move(propertyVectors)}, nodeIDVector{nodeIDVector},
          pkVector{pkVector} {}
};

struct NodeTableUpdateState : public TableUpdateState {
    const common::ValueVector& nodeIDVector;

    NodeTableUpdateState(common::column_id_t columnID, const common::ValueVector& nodeIDVector,
        const common::ValueVector& propertyVector)
        : TableUpdateState{columnID, propertyVector}, nodeIDVector{nodeIDVector} {}
};

struct NodeTableDeleteState : public TableDeleteState {
    const common::ValueVector& nodeIDVector;
    common::ValueVector& pkVector;

    explicit NodeTableDeleteState(const common::ValueVector& nodeIDVector,
        common::ValueVector& pkVector)
        : nodeIDVector{nodeIDVector}, pkVector{pkVector} {}
};

class NodeTable final : public Table {
public:
    NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        catalog::NodeTableCatalogEntry* nodeTableEntry,
        NodesStoreStatsAndDeletedIDs* nodesStatisticsAndDeletedIDs, MemoryManager* memoryManager,
        WAL* wal, bool readOnly, bool enableCompression, common::VirtualFileSystem* vfs);

    void initializePKIndex(catalog::NodeTableCatalogEntry* nodeTableEntry, bool readOnly,
        common::VirtualFileSystem* vfs);

    inline common::offset_t getMaxNodeOffset(transaction::Transaction* transaction) const {
        auto nodesStats = common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(
            tablesStatistics);
        return nodesStats->getMaxNodeOffset(transaction, tableID);
    }
    void setSelVectorForDeletedOffsets(transaction::Transaction* trx,
        common::ValueVector* vector) const {
        KU_ASSERT(vector->isSequential());
        auto nodeStateCollection =
            common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(
                tablesStatistics);
        nodeStateCollection->setDeletedNodeOffsetsForMorsel(trx, vector, tableID);
    }

    inline void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, const common::ValueVector& inNodeIDVector,
        TableReadState& readState) {
        tableData->initializeReadState(transaction, std::move(columnIDs), inNodeIDVector,
            *readState.dataReadState);
    }
    void read(transaction::Transaction* transaction, TableReadState& readState) override;

    // Return the max node offset during insertions.
    common::offset_t validateUniquenessConstraint(transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& propertyVectors);

    void insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    void update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    void delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) override;
    inline void dropColumn(common::column_id_t columnID) override {
        tableData->dropColumn(columnID);
    }

    inline common::column_id_t getPKColumnID() const { return pkColumnID; }
    inline PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }
    inline NodesStoreStatsAndDeletedIDs* getNodeStatisticsAndDeletedIDs() const {
        return common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(
            tablesStatistics);
    }
    inline common::column_id_t getNumColumns() const { return tableData->getNumColumns(); }
    inline Column* getColumn(common::column_id_t columnID) {
        return tableData->getColumn(columnID);
    }

    inline void append(ChunkedNodeGroup* nodeGroup) { tableData->append(nodeGroup); }

    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void prepareCommit() override;
    void prepareRollback(LocalTable* localTable) override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

private:
    void updatePK(transaction::Transaction* transaction, common::column_id_t columnID,
        const common::ValueVector& nodeIDVector, const common::ValueVector& pkVector);
    void insertPK(const common::ValueVector& nodeIDVector,
        const common::ValueVector& primaryKeyVector);

    void scan(transaction::Transaction* transaction, TableReadState& readState);
    void lookup(transaction::Transaction* transaction, TableReadState& readState);

private:
    std::unique_ptr<NodeTableData> tableData;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
};

} // namespace storage
} // namespace kuzu
