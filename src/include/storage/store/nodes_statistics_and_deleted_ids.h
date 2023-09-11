#pragma once

#include <map>
#include <set>

#include "catalog/node_table_schema.h"
#include "storage/store/rels_store.h"

namespace kuzu {
namespace storage {

class NodeStatisticsAndDeletedIDs : public TableStatistics {

public:
    explicit NodeStatisticsAndDeletedIDs(const catalog::TableSchema& schema)
        : TableStatistics{schema}, tableID{schema.tableID} {}

    NodeStatisticsAndDeletedIDs(common::table_id_t tableID, common::offset_t maxNodeOffset,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStatistics)
        : NodeStatisticsAndDeletedIDs(tableID, maxNodeOffset,
              std::vector<common::offset_t>() /* no deleted node offsets during initial loading */,
              std::move(propertyStatistics)) {}

    NodeStatisticsAndDeletedIDs(common::table_id_t tableID, common::offset_t maxNodeOffset,
        const std::vector<common::offset_t>& deletedNodeOffsets,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStatistics);

    NodeStatisticsAndDeletedIDs(const NodeStatisticsAndDeletedIDs& other) = default;

    inline common::offset_t getMaxNodeOffset() {
        return getMaxNodeOffsetFromNumTuples(getNumTuples());
    }

    inline void setAdjListsAndColumns(
        std::pair<std::vector<AdjLists*>, std::vector<Column*>> adjListsAndColumns_) {
        adjListsAndColumns = std::move(adjListsAndColumns_);
    }

    common::offset_t addNode();

    void deleteNode(common::offset_t nodeOffset);

    // This function assumes that it is being called right after ScanNodeID has obtained a
    // morsel and that the nodeID structs in nodeOffsetVector.values have consecutive node
    // offsets and the same tableID.
    void setDeletedNodeOffsetsForMorsel(
        const std::shared_ptr<common::ValueVector>& nodeOffsetVector);

    void setNumTuples(uint64_t numTuples) override;

    std::vector<common::offset_t> getDeletedNodeOffsets();

    static inline uint64_t getNumTuplesFromMaxNodeOffset(common::offset_t maxNodeOffset) {
        return (maxNodeOffset == UINT64_MAX) ? 0ull : maxNodeOffset + 1ull;
    }

    static inline uint64_t getMaxNodeOffsetFromNumTuples(uint64_t numTuples) {
        return numTuples == 0 ? UINT64_MAX : numTuples - 1;
    }

private:
    void errorIfNodeHasEdges(common::offset_t nodeOffset);

    // We pass the morselIdx to not do the division nodeOffset/DEFAULT_VECTOR_CAPACITY again
    bool isDeleted(common::offset_t nodeOffset, uint64_t morselIdx);

private:
    common::table_id_t tableID;
    // Note: This is initialized explicitly through a call to setAdjListsAndColumns after
    // construction.
    std::pair<std::vector<AdjLists*>, std::vector<Column*>> adjListsAndColumns;
    std::vector<bool> hasDeletedNodesPerMorsel;
    std::map<uint64_t, std::set<common::offset_t>> deletedNodeOffsetsPerMorsel;
};

// Manages the disk image of the maxNodeOffsets and deleted node IDs (per node table).
// Note: This class is *not* thread-safe.
class NodesStatisticsAndDeletedIDs : public TablesStatistics {

public:
    // Should only be used by saveInitialNodesStatisticsAndDeletedIDsToFile to start a database
    // from an empty directory.
    NodesStatisticsAndDeletedIDs() : TablesStatistics{} {};
    // Should be used when an already loaded database is started from a directory.
    explicit NodesStatisticsAndDeletedIDs(
        const std::string& directory, common::DBFileType dbFileType = common::DBFileType::ORIGINAL)
        : TablesStatistics{} {
        readFromFile(directory, dbFileType);
    }

    // Should be used only by tests;
    explicit NodesStatisticsAndDeletedIDs(
        std::unordered_map<common::table_id_t, std::unique_ptr<NodeStatisticsAndDeletedIDs>>&
            nodesStatisticsAndDeletedIDs);

    inline NodeStatisticsAndDeletedIDs* getNodeStatisticsAndDeletedIDs(
        common::table_id_t tableID) const {
        return (NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForReadOnlyTrx
            ->tableStatisticPerTable[tableID]
            .get();
    }

    inline void rollbackInMemoryIfNecessary() {
        lock_t lck{mtx};
        tablesStatisticsContentForWriteTrx.reset();
    }

    static inline void saveInitialNodesStatisticsAndDeletedIDsToFile(const std::string& directory) {
        std::make_unique<NodesStatisticsAndDeletedIDs>()->saveToFile(
            directory, common::DBFileType::ORIGINAL, transaction::TransactionType::READ_ONLY);
    }

    inline void setNumTuplesForTable(common::table_id_t tableID, uint64_t numTuples) override {
        initTableStatisticsForWriteTrx();
        ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                ->tableStatisticPerTable[tableID]
                .get())
            ->setNumTuples(numTuples);
    }

    inline common::offset_t getMaxNodeOffset(
        transaction::Transaction* transaction, common::table_id_t tableID) {
        assert(transaction);
        if (transaction->getType() == transaction::TransactionType::READ_ONLY) {
            return getNodeStatisticsAndDeletedIDs(tableID)->getMaxNodeOffset();
        } else {
            std::unique_lock xLck{mtx};
            return tablesStatisticsContentForWriteTrx == nullptr ?
                       getNodeStatisticsAndDeletedIDs(tableID)->getMaxNodeOffset() :
                       ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                               ->tableStatisticPerTable[tableID]
                               .get())
                           ->getMaxNodeOffset();
        }
    }

    // This function is only used for testing purpose.
    inline uint32_t getNumNodeStatisticsAndDeleteIDsPerTable() const {
        return tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.size();
    }

    void setAdjListsAndColumns(RelsStore* relsStore);

    // This function assumes that there is a single write transaction. That is why for now we
    // keep the interface simple and no transaction is passed.
    common::offset_t addNode(common::table_id_t tableID) {
        lock_t lck{mtx};
        initTableStatisticsForWriteTrxNoLock();
        return ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                    ->tableStatisticPerTable[tableID]
                    .get())
            ->addNode();
    }

    // Refer to the comments for addNode.
    void deleteNode(common::table_id_t tableID, common::offset_t nodeOffset) {
        lock_t lck{mtx};
        initTableStatisticsForWriteTrxNoLock();
        ((NodeStatisticsAndDeletedIDs*)tablesStatisticsContentForWriteTrx
                ->tableStatisticPerTable[tableID]
                .get())
            ->deleteNode(nodeOffset);
    }

    // This function is only used by storageManager to construct relsStore during start-up, so
    // we can just safely return the maxNodeOffsetPerTable for readOnlyVersion.
    std::map<common::table_id_t, common::offset_t> getMaxNodeOffsetPerTable() const;

    void setDeletedNodeOffsetsForMorsel(transaction::Transaction* transaction,
        const std::shared_ptr<common::ValueVector>& nodeOffsetVector, common::table_id_t tableID);

    void addNodeStatisticsAndDeletedIDs(catalog::NodeTableSchema* tableSchema);

protected:
    inline std::string getTableTypeForPrinting() const override {
        return "NodesStatisticsAndDeletedIDs";
    }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) override {
        return std::make_unique<NodeStatisticsAndDeletedIDs>(*tableSchema);
    }

    inline std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return std::make_unique<NodeStatisticsAndDeletedIDs>(
            *(NodeStatisticsAndDeletedIDs*)tableStatistics);
    }

    inline std::string getTableStatisticsFilePath(
        const std::string& directory, common::DBFileType dbFileType) override {
        return StorageUtils::getNodesStatisticsAndDeletedIDsFilePath(directory, dbFileType);
    }

    std::unique_ptr<TableStatistics> deserializeTableStatistics(uint64_t numTuples,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStats,
        uint64_t& offset, common::FileInfo* fileInfo, uint64_t tableID) override;

    void serializeTableStatistics(
        TableStatistics* tableStatistics, uint64_t& offset, common::FileInfo* fileInfo) override;
};

} // namespace storage
} // namespace kuzu
