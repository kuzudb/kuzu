#pragma once

#include "catalog/catalog.h"
#include "common/utils.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

// TODO(Guodong): remove the distinction between AdjColumn and Column, also AdjLists and Lists.
using table_adj_columns_map_t = unordered_map<table_id_t, unique_ptr<AdjColumn>>;
using table_adj_lists_map_t = unordered_map<table_id_t, unique_ptr<AdjLists>>;
using table_property_columns_map_t =
    unordered_map<table_id_t, unordered_map<property_id_t, unique_ptr<Column>>>;
using table_property_lists_map_t =
    unordered_map<table_id_t, unordered_map<property_id_t, unique_ptr<Lists>>>;

enum class RelTableDataType : uint8_t {
    COLUMNS = 0,
    LISTS = 1,
};

class ListsUpdateIteratorsForDirection {
public:
    inline ListsUpdateIterator* getListUpdateIteratorForAdjList() {
        return listUpdateIteratorForAdjList.get();
    }

    inline unordered_map<property_id_t, unique_ptr<ListsUpdateIterator>>&
    getListsUpdateIteratorsForPropertyLists() {
        return listUpdateIteratorsForPropertyLists;
    }

    void addListUpdateIteratorForAdjList(unique_ptr<ListsUpdateIterator>);

    void addListUpdateIteratorForPropertyList(
        property_id_t propertyID, unique_ptr<ListsUpdateIterator> listsUpdateIterator);

    void doneUpdating();

private:
    unique_ptr<ListsUpdateIterator> listUpdateIteratorForAdjList;
    unordered_map<property_id_t, unique_ptr<ListsUpdateIterator>>
        listUpdateIteratorsForPropertyLists;
};

struct RelTableScanState {
public:
    explicit RelTableScanState(table_id_t boundNodeTableID, vector<property_id_t> propertyIds,
        RelTableDataType relTableDataType)
        : relTableDataType{relTableDataType}, boundNodeTableID{boundNodeTableID},
          propertyIds{std::move(propertyIds)} {
        if (relTableDataType == RelTableDataType::LISTS) {
            syncState = make_unique<ListSyncState>();
            // The first listHandle is for adj lists.
            listHandles.resize(this->propertyIds.size() + 1);
            for (auto i = 0u; i < this->propertyIds.size() + 1; i++) {
                listHandles[i] = make_unique<ListHandle>(*syncState);
            }
        }
    }

    bool hasMoreAndSwitchSourceIfNecessary() const {
        return relTableDataType == RelTableDataType::LISTS &&
               syncState->hasMoreAndSwitchSourceIfNecessary();
    }

    RelTableDataType relTableDataType;
    table_id_t boundNodeTableID;
    vector<property_id_t> propertyIds;
    // sync state between adj and property lists
    unique_ptr<ListSyncState> syncState;
    vector<unique_ptr<ListHandle>> listHandles;
};

class DirectedRelTableData {
public:
    DirectedRelTableData(table_id_t tableID, RelDirection direction,
        ListsUpdatesStore* listsUpdatesStore, BufferManager& bufferManager)
        : tableID{tableID}, direction{direction}, listsUpdatesStore{listsUpdatesStore},
          bufferManager{bufferManager} {}

    inline bool hasAdjColumn(table_id_t boundNodeTableID) {
        return adjColumns.contains(boundNodeTableID);
    }
    inline bool hasAdjLists(table_id_t boundNodeTableID) {
        return adjLists.contains(boundNodeTableID);
    }
    inline uint32_t getNumPropertyLists(table_id_t boundNodeTableID) {
        return propertyLists.at(boundNodeTableID).size();
    }
    // Returns the list offset of the given relID if the relID stored as list in the current
    // direction, otherwise it returns UINT64_MAX.
    inline list_offset_t getListOffset(nodeID_t nodeID, int64_t relID) {
        return propertyLists.contains(nodeID.tableID) ?
                   ((RelIDList*)getPropertyLists(
                        nodeID.tableID, RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX))
                       ->getListOffset(nodeID.offset, relID) :
                   UINT64_MAX;
    }

    void initializeData(RelTableSchema* tableSchema, WAL* wal);
    void initializeColumnsForBoundNodeTable(RelTableSchema* tableSchema,
        table_id_t boundNodeTableID, NodeIDCompressionScheme& nodeIDCompressionScheme,
        BufferManager& bufferManager, WAL* wal);
    void initializeListsForBoundNodeTabl(RelTableSchema* tableSchema, table_id_t boundNodeTableID,
        NodeIDCompressionScheme& nodeIDCompressionScheme, BufferManager& bufferManager, WAL* wal);
    Column* getPropertyColumn(table_id_t boundNodeTableID, property_id_t propertyID);
    Lists* getPropertyLists(table_id_t boundNodeTableID, property_id_t propertyID);
    AdjColumn* getAdjColumn(table_id_t boundNodeTableID);
    AdjLists* getAdjLists(table_id_t boundNodeTableID);

    inline void scan(Transaction* transaction, RelTableScanState& scanState,
        const shared_ptr<ValueVector>& inNodeIDVector,
        vector<shared_ptr<ValueVector>>& outputVectors) {
        if (scanState.relTableDataType == RelTableDataType::COLUMNS) {
            scanColumns(transaction, scanState, inNodeIDVector, outputVectors);
        } else {
            scanLists(transaction, scanState, inNodeIDVector, outputVectors);
        }
    }

    void insertRel(const shared_ptr<ValueVector>& boundVector,
        const shared_ptr<ValueVector>& nbrVector,
        const vector<shared_ptr<ValueVector>>& relPropertyVectors);
    void deleteRel(const shared_ptr<ValueVector>& boundVector);
    void updateRel(const shared_ptr<ValueVector>& boundVector, property_id_t propertyID,
        const shared_ptr<ValueVector>& propertyVector);
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates);
    unique_ptr<ListsUpdateIteratorsForDirection> getListsUpdateIteratorsForDirection(
        table_id_t boundNodeTableID);
    void removeProperty(property_id_t propertyID);
    void addProperty(Property& property, WAL* wal);
    void batchInitEmptyRelsForNewNodes(const RelTableSchema* relTableSchema,
        table_id_t boundTableID, uint64_t numNodesInTable, const string& directory);

private:
    void scanColumns(Transaction* transaction, RelTableScanState& scanState,
        const shared_ptr<ValueVector>& inNodeIDVector,
        vector<shared_ptr<ValueVector>>& outputVectors);
    void scanLists(Transaction* transaction, RelTableScanState& scanState,
        const shared_ptr<ValueVector>& inNodeIDVector,
        vector<shared_ptr<ValueVector>>& outputVectors);

private:
    table_property_columns_map_t propertyColumns;
    table_adj_columns_map_t adjColumns;
    table_property_lists_map_t propertyLists;
    table_adj_lists_map_t adjLists;
    table_id_t tableID;
    RelDirection direction;
    ListsUpdatesStore* listsUpdatesStore;
    BufferManager& bufferManager;
};

class RelTable {
public:
    RelTable(const catalog::Catalog& catalog, table_id_t tableID, BufferManager& bufferManager,
        MemoryManager& memoryManager, WAL* wal);

    void initializeData(RelTableSchema* tableSchema);

    inline Column* getPropertyColumn(
        RelDirection relDirection, table_id_t boundNodeTableID, property_id_t propertyId) {
        return relDirection == FWD ?
                   fwdRelTableData->getPropertyColumn(boundNodeTableID, propertyId) :
                   bwdRelTableData->getPropertyColumn(boundNodeTableID, propertyId);
    }
    inline Lists* getPropertyLists(
        RelDirection relDirection, table_id_t boundNodeTableID, property_id_t propertyId) {
        return relDirection == FWD ?
                   fwdRelTableData->getPropertyLists(boundNodeTableID, propertyId) :
                   bwdRelTableData->getPropertyLists(boundNodeTableID, propertyId);
    }
    inline uint32_t getNumPropertyLists(RelDirection relDirection, table_id_t boundNodeTableID) {
        return relDirection == FWD ? fwdRelTableData->getNumPropertyLists(boundNodeTableID) :
                                     bwdRelTableData->getNumPropertyLists(boundNodeTableID);
    }
    inline bool hasAdjColumn(RelDirection relDirection, table_id_t boundNodeTableID) {
        return relDirection == FWD ? fwdRelTableData->hasAdjColumn(boundNodeTableID) :
                                     bwdRelTableData->hasAdjColumn(boundNodeTableID);
    }
    inline AdjColumn* getAdjColumn(RelDirection relDirection, table_id_t boundNodeTableID) {
        return relDirection == FWD ? fwdRelTableData->getAdjColumn(boundNodeTableID) :
                                     bwdRelTableData->getAdjColumn(boundNodeTableID);
    }
    inline bool hasAdjList(RelDirection relDirection, table_id_t boundNodeTableID) {
        return relDirection == FWD ? fwdRelTableData->hasAdjLists(boundNodeTableID) :
                                     bwdRelTableData->hasAdjLists(boundNodeTableID);
    }
    inline AdjLists* getAdjLists(RelDirection relDirection, table_id_t boundNodeTableID) {
        return relDirection == FWD ? fwdRelTableData->getAdjLists(boundNodeTableID) :
                                     bwdRelTableData->getAdjLists(boundNodeTableID);
    }
    inline table_id_t getRelTableID() const { return tableID; }
    inline DirectedRelTableData* getDirectedTableData(RelDirection relDirection) {
        return relDirection == FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    }
    inline void removeProperty(property_id_t propertyID) {
        fwdRelTableData->removeProperty(propertyID);
        bwdRelTableData->removeProperty(propertyID);
    }

    vector<AdjLists*> getAdjListsForNodeTable(table_id_t tableID);
    vector<AdjColumn*> getAdjColumnsForNodeTable(table_id_t tableID);

    void prepareCommitOrRollbackIfNecessary(bool isCommit);
    void checkpointInMemoryIfNecessary();
    void rollbackInMemoryIfNecessary();

    void insertRel(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector,
        const vector<shared_ptr<ValueVector>>& relPropertyVectors);
    void deleteRel(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector);
    void updateRel(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector,
        const shared_ptr<ValueVector>& propertyVector, uint32_t propertyID);
    void initEmptyRelsForNewNode(nodeID_t& nodeID);
    void batchInitEmptyRelsForNewNodes(
        const RelTableSchema* relTableSchema, table_id_t boundTableID, uint64_t numNodesInTable);
    void addProperty(Property property);

private:
    inline void addToUpdatedRelTables() { wal->addToUpdatedRelTables(tableID); }
    inline void clearListsUpdatesStore() { listsUpdatesStore->clear(); }
    static void appendInMemListToLargeListOP(
        ListsUpdateIterator* listsUpdateIterator, offset_t nodeOffset, InMemList& inMemList);
    static void updateListOP(
        ListsUpdateIterator* listsUpdateIterator, offset_t nodeOffset, InMemList& inMemList);
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates,
        const std::function<void()>& opIfHasUpdates);
    unique_ptr<ListsUpdateIteratorsForDirection> getListsUpdateIteratorsForDirection(
        RelDirection relDirection, table_id_t boundNodeTableID) const;
    void prepareCommitForDirection(RelDirection relDirection);
    void prepareCommitForListWithUpdateStoreDataOnly(AdjLists* adjLists, offset_t nodeOffset,
        ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, RelDirection relDirection,
        ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
        table_id_t boundNodeTableID,
        const std::function<void(ListsUpdateIterator* listsUpdateIterator, offset_t,
            InMemList& inMemList)>& opOnListsUpdateIterators);
    void prepareCommitForList(AdjLists* adjLists, offset_t nodeOffset,
        ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, RelDirection relDirection,
        ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
        table_id_t boundNodeTableID);

private:
    table_id_t tableID;
    unique_ptr<DirectedRelTableData> fwdRelTableData;
    unique_ptr<DirectedRelTableData> bwdRelTableData;
    unique_ptr<ListsUpdatesStore> listsUpdatesStore;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
