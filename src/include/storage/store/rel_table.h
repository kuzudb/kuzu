#pragma once

#include "catalog/catalog.h"
#include "common/utils.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

// TODO(Guodong): remove the distinction between AdjColumn and Column, also AdjLists and Lists.
using table_adj_columns_map_t = std::unordered_map<common::table_id_t, std::unique_ptr<AdjColumn>>;
using table_adj_lists_map_t = std::unordered_map<common::table_id_t, std::unique_ptr<AdjLists>>;
using table_property_columns_map_t = std::unordered_map<common::table_id_t,
    std::unordered_map<common::property_id_t, std::unique_ptr<Column>>>;
using table_property_lists_map_t = std::unordered_map<common::table_id_t,
    std::unordered_map<common::property_id_t, std::unique_ptr<Lists>>>;

enum class RelTableDataType : uint8_t {
    COLUMNS = 0,
    LISTS = 1,
};

class ListsUpdateIteratorsForDirection {
public:
    inline ListsUpdateIterator* getListUpdateIteratorForAdjList() {
        return listUpdateIteratorForAdjList.get();
    }

    inline std::unordered_map<common::property_id_t, std::unique_ptr<ListsUpdateIterator>>&
    getListsUpdateIteratorsForPropertyLists() {
        return listUpdateIteratorsForPropertyLists;
    }

    void addListUpdateIteratorForAdjList(std::unique_ptr<ListsUpdateIterator>);

    void addListUpdateIteratorForPropertyList(
        common::property_id_t propertyID, std::unique_ptr<ListsUpdateIterator> listsUpdateIterator);

    void doneUpdating();

private:
    std::unique_ptr<ListsUpdateIterator> listUpdateIteratorForAdjList;
    std::unordered_map<common::property_id_t, std::unique_ptr<ListsUpdateIterator>>
        listUpdateIteratorsForPropertyLists;
};

struct RelTableScanState {
public:
    explicit RelTableScanState(common::table_id_t boundNodeTableID,
        std::vector<common::property_id_t> propertyIds, RelTableDataType relTableDataType)
        : relTableDataType{relTableDataType}, boundNodeTableID{boundNodeTableID},
          propertyIds{std::move(propertyIds)} {
        if (relTableDataType == RelTableDataType::LISTS) {
            syncState = std::make_unique<ListSyncState>();
            // The first listHandle is for adj lists.
            listHandles.resize(this->propertyIds.size() + 1);
            for (auto i = 0u; i < this->propertyIds.size() + 1; i++) {
                listHandles[i] = std::make_unique<ListHandle>(*syncState);
            }
        }
    }

    bool hasMoreAndSwitchSourceIfNecessary() const {
        return relTableDataType == RelTableDataType::LISTS &&
               syncState->hasMoreAndSwitchSourceIfNecessary();
    }

    RelTableDataType relTableDataType;
    common::table_id_t boundNodeTableID;
    std::vector<common::property_id_t> propertyIds;
    // sync state between adj and property lists
    std::unique_ptr<ListSyncState> syncState;
    std::vector<std::unique_ptr<ListHandle>> listHandles;
};

class DirectedRelTableData {
public:
    DirectedRelTableData(common::table_id_t tableID, common::RelDirection direction,
        ListsUpdatesStore* listsUpdatesStore, BufferManager& bufferManager)
        : tableID{tableID}, direction{direction}, listsUpdatesStore{listsUpdatesStore},
          bufferManager{bufferManager} {}

    inline bool hasAdjColumn(common::table_id_t boundNodeTableID) {
        return adjColumns.contains(boundNodeTableID);
    }
    inline bool hasAdjLists(common::table_id_t boundNodeTableID) {
        return adjLists.contains(boundNodeTableID);
    }
    inline uint32_t getNumPropertyLists(common::table_id_t boundNodeTableID) {
        return propertyLists.at(boundNodeTableID).size();
    }
    // Returns the list offset of the given relID if the relID stored as list in the current
    // direction, otherwise it returns UINT64_MAX.
    inline list_offset_t getListOffset(common::nodeID_t nodeID, int64_t relID) {
        return propertyLists.contains(nodeID.tableID) ?
                   ((RelIDList*)getPropertyLists(
                        nodeID.tableID, catalog::RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX))
                       ->getListOffset(nodeID.offset, relID) :
                   UINT64_MAX;
    }

    void initializeData(catalog::RelTableSchema* tableSchema, WAL* wal);
    void initializeColumnsForBoundNodeTable(catalog::RelTableSchema* tableSchema,
        common::table_id_t boundNodeTableID, NodeIDCompressionScheme& nodeIDCompressionScheme,
        BufferManager& bufferManager, WAL* wal);
    void initializeListsForBoundNodeTabl(catalog::RelTableSchema* tableSchema,
        common::table_id_t boundNodeTableID, NodeIDCompressionScheme& nodeIDCompressionScheme,
        BufferManager& bufferManager, WAL* wal);
    Column* getPropertyColumn(
        common::table_id_t boundNodeTableID, common::property_id_t propertyID);
    Lists* getPropertyLists(common::table_id_t boundNodeTableID, common::property_id_t propertyID);
    AdjColumn* getAdjColumn(common::table_id_t boundNodeTableID);
    AdjLists* getAdjLists(common::table_id_t boundNodeTableID);

    inline void scan(transaction::Transaction* transaction, RelTableScanState& scanState,
        const std::shared_ptr<common::ValueVector>& inNodeIDVector,
        std::vector<std::shared_ptr<common::ValueVector>>& outputVectors) {
        if (scanState.relTableDataType == RelTableDataType::COLUMNS) {
            scanColumns(transaction, scanState, inNodeIDVector, outputVectors);
        } else {
            scanLists(transaction, scanState, inNodeIDVector, outputVectors);
        }
    }

    void insertRel(const std::shared_ptr<common::ValueVector>& boundVector,
        const std::shared_ptr<common::ValueVector>& nbrVector,
        const std::vector<std::shared_ptr<common::ValueVector>>& relPropertyVectors);
    void deleteRel(const std::shared_ptr<common::ValueVector>& boundVector);
    void updateRel(const std::shared_ptr<common::ValueVector>& boundVector,
        common::property_id_t propertyID,
        const std::shared_ptr<common::ValueVector>& propertyVector);
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates);
    std::unique_ptr<ListsUpdateIteratorsForDirection> getListsUpdateIteratorsForDirection(
        common::table_id_t boundNodeTableID);
    void removeProperty(common::property_id_t propertyID);
    void addProperty(catalog::Property& property, WAL* wal);
    void batchInitEmptyRelsForNewNodes(const catalog::RelTableSchema* relTableSchema,
        common::table_id_t boundTableID, uint64_t numNodesInTable, const std::string& directory);

private:
    void scanColumns(transaction::Transaction* transaction, RelTableScanState& scanState,
        const std::shared_ptr<common::ValueVector>& inNodeIDVector,
        std::vector<std::shared_ptr<common::ValueVector>>& outputVectors);
    void scanLists(transaction::Transaction* transaction, RelTableScanState& scanState,
        const std::shared_ptr<common::ValueVector>& inNodeIDVector,
        std::vector<std::shared_ptr<common::ValueVector>>& outputVectors);

private:
    table_property_columns_map_t propertyColumns;
    table_adj_columns_map_t adjColumns;
    table_property_lists_map_t propertyLists;
    table_adj_lists_map_t adjLists;
    common::table_id_t tableID;
    common::RelDirection direction;
    ListsUpdatesStore* listsUpdatesStore;
    BufferManager& bufferManager;
};

class RelTable {
public:
    RelTable(const catalog::Catalog& catalog, common::table_id_t tableID,
        BufferManager& bufferManager, MemoryManager& memoryManager, WAL* wal);

    void initializeData(catalog::RelTableSchema* tableSchema);

    inline Column* getPropertyColumn(common::RelDirection relDirection,
        common::table_id_t boundNodeTableID, common::property_id_t propertyId) {
        return relDirection == common::FWD ?
                   fwdRelTableData->getPropertyColumn(boundNodeTableID, propertyId) :
                   bwdRelTableData->getPropertyColumn(boundNodeTableID, propertyId);
    }
    inline Lists* getPropertyLists(common::RelDirection relDirection,
        common::table_id_t boundNodeTableID, common::property_id_t propertyId) {
        return relDirection == common::FWD ?
                   fwdRelTableData->getPropertyLists(boundNodeTableID, propertyId) :
                   bwdRelTableData->getPropertyLists(boundNodeTableID, propertyId);
    }
    inline uint32_t getNumPropertyLists(
        common::RelDirection relDirection, common::table_id_t boundNodeTableID) {
        return relDirection == common::FWD ?
                   fwdRelTableData->getNumPropertyLists(boundNodeTableID) :
                   bwdRelTableData->getNumPropertyLists(boundNodeTableID);
    }
    inline bool hasAdjColumn(
        common::RelDirection relDirection, common::table_id_t boundNodeTableID) {
        return relDirection == common::FWD ? fwdRelTableData->hasAdjColumn(boundNodeTableID) :
                                             bwdRelTableData->hasAdjColumn(boundNodeTableID);
    }
    inline AdjColumn* getAdjColumn(
        common::RelDirection relDirection, common::table_id_t boundNodeTableID) {
        return relDirection == common::FWD ? fwdRelTableData->getAdjColumn(boundNodeTableID) :
                                             bwdRelTableData->getAdjColumn(boundNodeTableID);
    }
    inline bool hasAdjList(common::RelDirection relDirection, common::table_id_t boundNodeTableID) {
        return relDirection == common::FWD ? fwdRelTableData->hasAdjLists(boundNodeTableID) :
                                             bwdRelTableData->hasAdjLists(boundNodeTableID);
    }
    inline AdjLists* getAdjLists(
        common::RelDirection relDirection, common::table_id_t boundNodeTableID) {
        return relDirection == common::FWD ? fwdRelTableData->getAdjLists(boundNodeTableID) :
                                             bwdRelTableData->getAdjLists(boundNodeTableID);
    }
    inline common::table_id_t getRelTableID() const { return tableID; }
    inline DirectedRelTableData* getDirectedTableData(common::RelDirection relDirection) {
        return relDirection == common::FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    }
    inline void removeProperty(common::property_id_t propertyID) {
        fwdRelTableData->removeProperty(propertyID);
        bwdRelTableData->removeProperty(propertyID);
    }

    std::vector<AdjLists*> getAdjListsForNodeTable(common::table_id_t tableID);
    std::vector<AdjColumn*> getAdjColumnsForNodeTable(common::table_id_t tableID);

    void prepareCommitOrRollbackIfNecessary(bool isCommit);
    void checkpointInMemoryIfNecessary();
    void rollbackInMemoryIfNecessary();

    void insertRel(const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
        const std::shared_ptr<common::ValueVector>& dstNodeIDVector,
        const std::vector<std::shared_ptr<common::ValueVector>>& relPropertyVectors);
    void deleteRel(const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
        const std::shared_ptr<common::ValueVector>& dstNodeIDVector,
        const std::shared_ptr<common::ValueVector>& relIDVector);
    void updateRel(const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
        const std::shared_ptr<common::ValueVector>& dstNodeIDVector,
        const std::shared_ptr<common::ValueVector>& relIDVector,
        const std::shared_ptr<common::ValueVector>& propertyVector, uint32_t propertyID);
    void initEmptyRelsForNewNode(common::nodeID_t& nodeID);
    void batchInitEmptyRelsForNewNodes(const catalog::RelTableSchema* relTableSchema,
        common::table_id_t boundTableID, uint64_t numNodesInTable);
    void addProperty(catalog::Property property);

private:
    inline void addToUpdatedRelTables() { wal->addToUpdatedRelTables(tableID); }
    inline void clearListsUpdatesStore() { listsUpdatesStore->clear(); }
    static void appendInMemListToLargeListOP(ListsUpdateIterator* listsUpdateIterator,
        common::offset_t nodeOffset, InMemList& inMemList);
    static void updateListOP(ListsUpdateIterator* listsUpdateIterator, common::offset_t nodeOffset,
        InMemList& inMemList);
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates,
        const std::function<void()>& opIfHasUpdates);
    std::unique_ptr<ListsUpdateIteratorsForDirection> getListsUpdateIteratorsForDirection(
        common::RelDirection relDirection, common::table_id_t boundNodeTableID) const;
    void prepareCommitForDirection(common::RelDirection relDirection);
    void prepareCommitForListWithUpdateStoreDataOnly(AdjLists* adjLists,
        common::offset_t nodeOffset, ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset,
        common::RelDirection relDirection,
        ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
        common::table_id_t boundNodeTableID,
        const std::function<void(ListsUpdateIterator* listsUpdateIterator, common::offset_t,
            InMemList& inMemList)>& opOnListsUpdateIterators);
    void prepareCommitForList(AdjLists* adjLists, common::offset_t nodeOffset,
        ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, common::RelDirection relDirection,
        ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
        common::table_id_t boundNodeTableID);

private:
    common::table_id_t tableID;
    std::unique_ptr<DirectedRelTableData> fwdRelTableData;
    std::unique_ptr<DirectedRelTableData> bwdRelTableData;
    std::unique_ptr<ListsUpdatesStore> listsUpdatesStore;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
