#pragma once

#include "catalog/catalog.h"
#include "common/utils.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

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
    RelTableScanState(
        std::vector<common::property_id_t> propertyIds, RelTableDataType relTableDataType)
        : relTableDataType{relTableDataType}, propertyIds{std::move(propertyIds)} {
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
    std::vector<common::property_id_t> propertyIds;
    // sync state between adj and property lists
    std::unique_ptr<ListSyncState> syncState;
    std::vector<std::unique_ptr<ListHandle>> listHandles;
};

class DirectedRelTableData {
public:
    DirectedRelTableData(common::table_id_t tableID, common::table_id_t boundTableID,
        common::RelDirection direction, ListsUpdatesStore* listsUpdatesStore,
        BufferManager& bufferManager, bool isSingleMultiplicityInDirection)
        : tableID{tableID}, boundTableID{boundTableID}, direction{direction},
          listsUpdatesStore{listsUpdatesStore}, bufferManager{bufferManager},
          isSingleMultiplicityInDirection{isSingleMultiplicityInDirection} {}

    inline uint32_t getNumPropertyLists() { return propertyLists.size(); }
    // Returns the list offset of the given relID if the relID stored as list in the current
    // direction, otherwise it returns UINT64_MAX.
    inline list_offset_t getListOffset(common::nodeID_t nodeID, int64_t relID) {
        return adjLists != nullptr ? ((RelIDList*)getPropertyLists(
                                          catalog::RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX))
                                         ->getListOffset(nodeID.offset, relID) :
                                     UINT64_MAX;
    }
    inline AdjColumn* getAdjColumn() const { return adjColumn.get(); }
    inline AdjLists* getAdjLists() const { return adjLists.get(); }
    inline bool isSingleMultiplicity() const { return isSingleMultiplicityInDirection; }

    void initializeData(catalog::RelTableSchema* tableSchema, WAL* wal);
    void initializeColumns(
        catalog::RelTableSchema* tableSchema, BufferManager& bufferManager, WAL* wal);
    void initializeLists(
        catalog::RelTableSchema* tableSchema, BufferManager& bufferManager, WAL* wal);
    Column* getPropertyColumn(common::property_id_t propertyID);
    Lists* getPropertyLists(common::property_id_t propertyID);

    inline void scan(transaction::Transaction* transaction, RelTableScanState& scanState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) {
        if (scanState.relTableDataType == RelTableDataType::COLUMNS) {
            scanColumns(transaction, scanState, inNodeIDVector, outputVectors);
        } else {
            scanLists(transaction, scanState, inNodeIDVector, outputVectors);
        }
    }
    inline bool isBoundTable(common::table_id_t tableID) const { return tableID == boundTableID; }

    void insertRel(common::ValueVector* boundVector, common::ValueVector* nbrVector,
        const std::vector<common::ValueVector*>& relPropertyVectors);
    void deleteRel(common::ValueVector* boundVector);
    void updateRel(common::ValueVector* boundVector, common::property_id_t propertyID,
        common::ValueVector* propertyVector);
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates);
    std::unique_ptr<ListsUpdateIteratorsForDirection> getListsUpdateIteratorsForDirection();
    void removeProperty(common::property_id_t propertyID);
    void addProperty(catalog::Property& property, WAL* wal);
    void batchInitEmptyRelsForNewNodes(const catalog::RelTableSchema* relTableSchema,
        uint64_t numNodesInTable, const std::string& directory);

private:
    void scanColumns(transaction::Transaction* transaction, RelTableScanState& scanState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);
    void scanLists(transaction::Transaction* transaction, RelTableScanState& scanState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

private:
    // TODO(Guodong): remove the distinction between AdjColumn and Column, also AdjLists and Lists.
    std::unordered_map<common::property_id_t, std::unique_ptr<Column>> propertyColumns;
    std::unique_ptr<AdjColumn> adjColumn;
    std::unordered_map<common::property_id_t, std::unique_ptr<Lists>> propertyLists;
    std::unique_ptr<AdjLists> adjLists;
    common::table_id_t tableID;
    common::table_id_t boundTableID;
    common::RelDirection direction;
    ListsUpdatesStore* listsUpdatesStore;
    BufferManager& bufferManager;
    // TODO(Guodong): remove this variable when removing the distinction between AdjColumn and
    // Column.
    bool isSingleMultiplicityInDirection;
};

class RelTable {
public:
    RelTable(const catalog::Catalog& catalog, common::table_id_t tableID,
        MemoryManager& memoryManager, WAL* wal);

    void initializeData(catalog::RelTableSchema* tableSchema);

    inline Column* getPropertyColumn(
        common::RelDirection relDirection, common::property_id_t propertyId) {
        return relDirection == common::RelDirection::FWD ?
                   fwdRelTableData->getPropertyColumn(propertyId) :
                   bwdRelTableData->getPropertyColumn(propertyId);
    }
    inline Lists* getPropertyLists(
        common::RelDirection relDirection, common::property_id_t propertyId) {
        return relDirection == common::RelDirection::FWD ?
                   fwdRelTableData->getPropertyLists(propertyId) :
                   bwdRelTableData->getPropertyLists(propertyId);
    }
    inline uint32_t getNumPropertyLists(common::RelDirection relDirection) {
        return relDirection == common::RelDirection::FWD ? fwdRelTableData->getNumPropertyLists() :
                                                           bwdRelTableData->getNumPropertyLists();
    }
    inline AdjColumn* getAdjColumn(common::RelDirection relDirection) {
        return relDirection == common::RelDirection::FWD ? fwdRelTableData->getAdjColumn() :
                                                           bwdRelTableData->getAdjColumn();
    }
    inline AdjLists* getAdjLists(common::RelDirection relDirection) {
        return relDirection == common::RelDirection::FWD ? fwdRelTableData->getAdjLists() :
                                                           bwdRelTableData->getAdjLists();
    }
    inline common::table_id_t getRelTableID() const { return tableID; }
    inline DirectedRelTableData* getDirectedTableData(common::RelDirection relDirection) {
        return relDirection == common::FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    }
    inline void removeProperty(
        common::property_id_t propertyID, catalog::RelTableSchema& relTableSchema) {
        fwdRelTableData->removeProperty(propertyID);
        bwdRelTableData->removeProperty(propertyID);
        listsUpdatesStore->updateSchema(relTableSchema);
    }
    inline bool isSingleMultiplicityInDirection(common::RelDirection relDirection) {
        return relDirection == common::RelDirection::FWD ? fwdRelTableData->isSingleMultiplicity() :
                                                           bwdRelTableData->isSingleMultiplicity();
    }

    std::vector<AdjLists*> getAllAdjLists(common::table_id_t boundTableID);
    std::vector<AdjColumn*> getAllAdjColumns(common::table_id_t boundTableID);

    void prepareCommitOrRollbackIfNecessary(bool isCommit);
    void checkpointInMemoryIfNecessary();
    void rollbackInMemoryIfNecessary();

    void insertRel(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& relPropertyVectors);
    void deleteRel(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector);
    void updateRel(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector, common::ValueVector* propertyVector, uint32_t propertyID);
    void initEmptyRelsForNewNode(common::nodeID_t& nodeID);
    void batchInitEmptyRelsForNewNodes(
        const catalog::RelTableSchema* relTableSchema, uint64_t numNodesInTable);
    void addProperty(catalog::Property property, catalog::RelTableSchema& relTableSchema);

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
        common::RelDirection relDirection) const;
    void prepareCommitForDirection(common::RelDirection relDirection);
    void prepareCommitForListWithUpdateStoreDataOnly(AdjLists* adjLists,
        common::offset_t nodeOffset, ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset,
        common::RelDirection relDirection,
        ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
        const std::function<void(ListsUpdateIterator* listsUpdateIterator, common::offset_t,
            InMemList& inMemList)>& opOnListsUpdateIterators);
    void prepareCommitForList(AdjLists* adjLists, common::offset_t nodeOffset,
        ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, common::RelDirection relDirection,
        ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection);

private:
    common::table_id_t tableID;
    std::unique_ptr<DirectedRelTableData> fwdRelTableData;
    std::unique_ptr<DirectedRelTableData> bwdRelTableData;
    std::unique_ptr<ListsUpdatesStore> listsUpdatesStore;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
