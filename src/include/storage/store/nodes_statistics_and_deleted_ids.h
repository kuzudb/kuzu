#pragma once

#include <map>
#include <set>

#include "storage/store/rels_store.h"

namespace kuzu {
namespace storage {

class NodeStatisticsAndDeletedIDs : public TableStatistics {

public:
    NodeStatisticsAndDeletedIDs(table_id_t tableID, node_offset_t maxNodeOffset)
        : NodeStatisticsAndDeletedIDs(tableID, maxNodeOffset,
              vector<node_offset_t>() /* no deleted node offsets during initial loading */) {}

    NodeStatisticsAndDeletedIDs(table_id_t tableID, node_offset_t maxNodeOffset,
        const vector<node_offset_t>& deletedNodeOffsets);

    NodeStatisticsAndDeletedIDs(const NodeStatisticsAndDeletedIDs& other)
        : TableStatistics{other.getNumTuples()}, tableID{other.tableID},
          adjListsAndColumns{other.adjListsAndColumns},
          hasDeletedNodesPerMorsel{other.hasDeletedNodesPerMorsel},
          deletedNodeOffsetsPerMorsel{other.deletedNodeOffsetsPerMorsel} {}

    inline node_offset_t getMaxNodeOffset() {
        return getMaxNodeOffsetFromNumTuples(getNumTuples());
    }

    inline void setAdjListsAndColumns(
        pair<vector<AdjLists*>, vector<AdjColumn*>> adjListsAndColumns_) {
        adjListsAndColumns = adjListsAndColumns_;
    }

    node_offset_t addNode();

    void deleteNode(node_offset_t nodeOffset);

    // This function assumes that it is being called right after ScanNodeID has obtained a
    // morsel and that the nodeID structs in nodeOffsetVector.values have consecutive node
    // offsets and the same tableID.
    void setDeletedNodeOffsetsForMorsel(const shared_ptr<ValueVector>& nodeOffsetVector);

    void setNumTuples(uint64_t numTuples) override;

    vector<node_offset_t> getDeletedNodeOffsets();

    static inline uint64_t geNumTuplesFromMaxNodeOffset(node_offset_t maxNodeOffset) {
        return (maxNodeOffset == UINT64_MAX) ? 0ull : maxNodeOffset + 1ull;
    }

    static inline uint64_t getMaxNodeOffsetFromNumTuples(uint64_t numTuples) {
        return numTuples == 0 ? UINT64_MAX : numTuples - 1;
    }

private:
    void errorIfNodeHasEdges(node_offset_t nodeOffset);

    // We pass the morselIdx to not do the division nodeOffset/DEFAULT_VECTOR_CAPACITY again
    bool isDeleted(node_offset_t nodeOffset, uint64_t morselIdx);

private:
    table_id_t tableID;
    // Note: This is initialized explicitly through a call to setAdjListsAndColumns after
    // construction.
    pair<vector<AdjLists*>, vector<AdjColumn*>> adjListsAndColumns;
    vector<bool> hasDeletedNodesPerMorsel;
    map<uint64_t, set<node_offset_t>> deletedNodeOffsetsPerMorsel;
};

// Manages the disk image of the maxNodeOffsets and deleted node IDs (per node table).
// Note: This class is *not* thread-safe.
class NodesStatisticsAndDeletedIDs : public TablesStatistics {

public:
    // Should only be used by saveInitialNodesStatisticsAndDeletedIDsToFile to start a database
    // from an empty directory.
    NodesStatisticsAndDeletedIDs() : TablesStatistics{} {};
    // Should be used when an already loaded database is started from a directory.
    explicit NodesStatisticsAndDeletedIDs(const string& directory) : TablesStatistics{} {
        logger->info("Initializing {}.", "NodesStatisticsAndDeletedIDs");
        readFromFile(directory);
        logger->info("Initialized {}.", "NodesStatisticsAndDeletedIDs");
    }

    // Should be used ony by tests;
    explicit NodesStatisticsAndDeletedIDs(
        unordered_map<table_id_t, unique_ptr<NodeStatisticsAndDeletedIDs>>&
            nodesStatisticsAndDeletedIDs);

    inline NodeStatisticsAndDeletedIDs* getNodeStatisticsAndDeletedIDs(table_id_t tableID) const {
        return (NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForReadOnlyTrx
            ->tableStatisticPerTable[tableID]
            .get();
    }

    inline void rollbackInMemoryIfNecessary() {
        lock_t lck{mtx};
        tablesStatisticsContentForWriteTrx.reset();
    }

    static inline void saveInitialNodesStatisticsAndDeletedIDsToFile(const string& directory) {
        make_unique<NodesStatisticsAndDeletedIDs>()->saveToFile(
            directory, DBFileType::ORIGINAL, TransactionType::READ_ONLY);
    }

    inline void setNumTuplesForTable(table_id_t tableID, uint64_t numTuples) {
        initTableStatisticPerTableForWriteTrxIfNecessary();
        ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                ->tableStatisticPerTable[tableID]
                .get())
            ->setNumTuples(numTuples);
    }

    inline node_offset_t getMaxNodeOffset(Transaction* transaction, table_id_t tableID) {
        return getMaxNodeOffset(transaction == nullptr || transaction->isReadOnly() ?
                                    TransactionType::READ_ONLY :
                                    TransactionType::WRITE,
            tableID);
    }

    inline node_offset_t getMaxNodeOffset(TransactionType transactionType, table_id_t tableID) {
        return (transactionType == TransactionType::READ_ONLY ||
                   tablesStatisticsContentForWriteTrx == nullptr) ?
                   getNodeStatisticsAndDeletedIDs(tableID)->getMaxNodeOffset() :
                   ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                           ->tableStatisticPerTable[tableID]
                           .get())
                       ->getMaxNodeOffset();
    }

    // This function is only used for testing purpose.
    inline uint32_t getNumNodeStatisticsAndDeleteIDsPerTable() const {
        return tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.size();
    }

    void setAdjListsAndColumns(RelsStore* relsStore);

    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.
    node_offset_t addNode(table_id_t tableID) {
        lock_t lck{mtx};
        initTableStatisticPerTableForWriteTrxIfNecessary();
        return ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                    ->tableStatisticPerTable[tableID]
                    .get())
            ->addNode();
    }

    // Refer to the comments for addNode.
    void deleteNode(table_id_t tableID, node_offset_t nodeOffset) {
        lock_t lck{mtx};
        initTableStatisticPerTableForWriteTrxIfNecessary();
        ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                ->tableStatisticPerTable[tableID]
                .get())
            ->deleteNode(nodeOffset);
    }

    // This function is only used by storageManager to construct relsStore during start-up, so
    // we can just safely return the maxNodeOffsetPerTable for readOnlyVersion.
    map<table_id_t, node_offset_t> getMaxNodeOffsetPerTable() const;

    void setDeletedNodeOffsetsForMorsel(Transaction* transaction,
        const shared_ptr<ValueVector>& nodeOffsetVector, table_id_t tableID);

    void addNodeStatisticsAndDeletedIDs(NodeTableSchema* tableSchema);

protected:
    inline string getTableTypeForPrinting() const override {
        return "NodesStatisticsAndDeletedIDs";
    }

    inline unique_ptr<TableStatistics> constructTableStatistic(TableSchema* tableSchema) override {
        // We use UINT64_MAX to represent an empty nodeTable which doesn't contain
        // any nodes.
        return make_unique<NodeStatisticsAndDeletedIDs>(
            tableSchema->tableID, UINT64_MAX /* maxNodeOffset */);
    }

    inline unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return make_unique<NodeStatisticsAndDeletedIDs>(
            *(NodeStatisticsAndDeletedIDs*)tableStatistics);
    }

    inline string getTableStatisticsFilePath(
        const string& directory, DBFileType dbFileType) override {
        return StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(directory, dbFileType);
    }

    unique_ptr<TableStatistics> deserializeTableStatistics(
        uint64_t numTuples, uint64_t& offset, FileInfo* fileInfo, uint64_t tableID) override;

    void serializeTableStatistics(
        TableStatistics* tableStatistics, uint64_t& offset, FileInfo* fileInfo) override;
};

} // namespace storage
} // namespace kuzu
