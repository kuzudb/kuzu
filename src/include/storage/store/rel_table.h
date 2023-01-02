#pragma once

#include "catalog/catalog.h"
#include "common/utils.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

using table_adj_columns_map_t = unordered_map<table_id_t, unique_ptr<AdjColumn>>;
using table_adj_lists_map_t = unordered_map<table_id_t, unique_ptr<AdjLists>>;
using table_property_columns_map_t = unordered_map<table_id_t, vector<unique_ptr<Column>>>;
using table_property_lists_map_t = unordered_map<table_id_t, vector<unique_ptr<Lists>>>;

class RelTable {

public:
    RelTable(const catalog::Catalog& catalog, table_id_t tableID, BufferManager& bufferManager,
        MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal);

    void loadColumnsAndListsFromDisk(const catalog::Catalog& catalog, BufferManager& bufferManager);

public:
    inline Column* getPropertyColumn(
        RelDirection relDirection, table_id_t boundNodeTableID, uint64_t propertyIdx) {
        return propertyColumns[relDirection].at(boundNodeTableID)[propertyIdx].get();
    }
    inline Lists* getPropertyLists(
        RelDirection relDirection, table_id_t boundNodeTableID, uint64_t propertyIdx) {
        return propertyLists[relDirection].at(boundNodeTableID)[propertyIdx].get();
    }
    inline bool hasAdjColumn(RelDirection relDirection, table_id_t boundNodeTableID) {
        return adjColumns[relDirection].contains(boundNodeTableID);
    }
    inline AdjColumn* getAdjColumn(RelDirection relDirection, table_id_t boundNodeTableID) {
        return adjColumns[relDirection].at(boundNodeTableID).get();
    }
    inline bool hasAdjList(RelDirection relDirection, table_id_t boundNodeTableID) {
        return adjLists[relDirection].contains(boundNodeTableID);
    }
    inline AdjLists* getAdjLists(RelDirection relDirection, table_id_t boundNodeTableID) {
        return adjLists[relDirection].at(boundNodeTableID).get();
    }
    inline ListsUpdateStore* getListsUpdateStore() { return listsUpdateStore.get(); }
    inline table_id_t getRelTableID() const { return tableID; }

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
    void initEmptyRelsForNewNode(nodeID_t& nodeID);

private:
    inline void addToUpdatedRelTables() { wal->addToUpdatedRelTables(tableID); }
    inline void clearListsUpdateStore() { listsUpdateStore->clear(); }
    void initAdjColumnOrLists(
        const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal);
    void initPropertyListsAndColumns(
        const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal);
    void initPropertyColumnsForRelTable(const catalog::Catalog& catalog, RelDirection relDirection,
        BufferManager& bufferManager, WAL* wal);
    void initPropertyListsForRelTable(const catalog::Catalog& catalog, RelDirection relDirection,
        BufferManager& bufferManager, WAL* wal);
    void performOpOnListsWithUpdates(
        std::function<void(Lists*)> opOnListsWithUpdates, std::function<void()> opIfHasUpdates);
    string inferRelMultiplicity(table_id_t srcTableID, table_id_t dstTableID);
    vector<unique_ptr<ListsUpdateIterator>> getListsUpdateIterators(
        RelDirection relDirection, table_id_t srcTableID) const;

private:
    shared_ptr<spdlog::logger> logger;
    table_id_t tableID;
    vector<table_property_columns_map_t> propertyColumns;
    vector<table_adj_columns_map_t> adjColumns;
    vector<table_property_lists_map_t> propertyLists;
    vector<table_adj_lists_map_t> adjLists;
    bool isInMemoryMode;
    unique_ptr<ListsUpdateStore> listsUpdateStore;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
