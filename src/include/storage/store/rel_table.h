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
using table_property_columns_map_t = unordered_map<table_id_t, vector<unique_ptr<Column>>>;
using table_property_lists_map_t = unordered_map<table_id_t, vector<unique_ptr<Lists>>>;

enum class RelTableDataType : uint8_t {
    COLUMNS = 0,
    LISTS = 1,
};

struct RelTableScanState {
public:
    explicit RelTableScanState(table_id_t boundNodeTableID, vector<uint32_t> propertyIds,
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
    vector<uint32_t> propertyIds;
    // sync state between adj and property lists
    unique_ptr<ListSyncState> syncState;
    vector<unique_ptr<ListHandle>> listHandles;
};

class DirectedRelTableData {
public:
    DirectedRelTableData(table_id_t tableID, RelDirection direction,
        ListsUpdatesStore* listsUpdatesStore, bool isInMemoryMode)
        : tableID{tableID}, direction{direction}, listsUpdatesStore{listsUpdatesStore},
          isInMemoryMode{isInMemoryMode} {}

    inline bool hasAdjColumn(table_id_t boundNodeTableID) {
        return adjColumns.contains(boundNodeTableID);
    }
    inline bool hasAdjLists(table_id_t boundNodeTableID) {
        return adjLists.contains(boundNodeTableID);
    }
    inline uint32_t getNumPropertyLists(table_id_t boundNodeTableID) {
        return propertyLists.at(boundNodeTableID).size();
    }
    inline list_offset_t getListOffset(nodeID_t nodeID, int64_t relID) {
        return ((RelIDList*)(propertyLists
                                 .at(nodeID.tableID)[RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX]
                                 .get()))
            ->getListOffset(nodeID.offset, relID);
    }

    void initializeData(RelTableSchema* tableSchema, BufferManager& bufferManager, WAL* wal);
    void initializeColumnsForBoundNodeTable(RelTableSchema* tableSchema,
        table_id_t boundNodeTableID, NodeIDCompressionScheme& nodeIDCompressionScheme,
        BufferManager& bufferManager, WAL* wal);
    void initializeListsForBoundNodeTabl(RelTableSchema* tableSchema, table_id_t boundNodeTableID,
        NodeIDCompressionScheme& nodeIDCompressionScheme, BufferManager& bufferManager, WAL* wal);
    Column* getPropertyColumn(table_id_t boundNodeTableID, uint64_t propertyIdx);
    Lists* getPropertyLists(table_id_t boundNodeTableID, uint64_t propertyIdx);
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

    void insertRel(table_id_t boundTableID, const shared_ptr<ValueVector>& boundVector,
        const shared_ptr<ValueVector>& nbrVector,
        const vector<shared_ptr<ValueVector>>& relPropertyVectors);
    void deleteRel(table_id_t boundTableID, const shared_ptr<ValueVector>& boundVector);
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates);
    vector<unique_ptr<ListsUpdateIterator>> getListsUpdateIterators(table_id_t srcTableID);

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
    bool isInMemoryMode;
};

class RelTable {
public:
    RelTable(const catalog::Catalog& catalog, table_id_t tableID, BufferManager& bufferManager,
        MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal);

    void initializeData(RelTableSchema* tableSchema, BufferManager& bufferManager);

    inline Column* getPropertyColumn(
        RelDirection relDirection, table_id_t boundNodeTableID, uint64_t propertyIdx) {
        return relDirection == FWD ?
                   fwdRelTableData->getPropertyColumn(boundNodeTableID, propertyIdx) :
                   bwdRelTableData->getPropertyColumn(boundNodeTableID, propertyIdx);
    }
    inline Lists* getPropertyLists(
        RelDirection relDirection, table_id_t boundNodeTableID, uint64_t propertyIdx) {
        return relDirection == FWD ?
                   fwdRelTableData->getPropertyLists(boundNodeTableID, propertyIdx) :
                   bwdRelTableData->getPropertyLists(boundNodeTableID, propertyIdx);
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

private:
    inline void addToUpdatedRelTables() { wal->addToUpdatedRelTables(tableID); }
    inline void clearListsUpdatesStore() { listsUpdatesStore->clear(); }
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates,
        const std::function<void()>& opIfHasUpdates);
    vector<unique_ptr<ListsUpdateIterator>> getListsUpdateIterators(
        RelDirection relDirection, table_id_t srcTableID) const;
    void prepareCommitForDirection(RelDirection relDirection);

private:
    table_id_t tableID;
    unique_ptr<DirectedRelTableData> fwdRelTableData;
    unique_ptr<DirectedRelTableData> bwdRelTableData;
    unique_ptr<ListsUpdatesStore> listsUpdatesStore;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
