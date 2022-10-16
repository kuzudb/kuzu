#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/utils.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/storage_structure/include/column.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace storage {

using table_adj_columns_map_t = unordered_map<table_id_t, unique_ptr<AdjColumn>>;
using table_adj_lists_map_t = unordered_map<table_id_t, unique_ptr<AdjLists>>;
using table_property_columns_map_t = unordered_map<table_id_t, vector<unique_ptr<Column>>>;
using table_property_lists_map_t =
    unordered_map<table_id_t, vector<unique_ptr<ListsWithAdjAndPropertyListsUpdateStore>>>;

class RelTable {

public:
    explicit RelTable(const catalog::Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerTable, table_id_t tableID,
        BufferManager& bufferManager, MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal);

    void loadColumnsAndListsFromDisk(const catalog::Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerTable, BufferManager& bufferManager);

public:
    inline Column* getPropertyColumn(
        RelDirection relDirection, table_id_t tableID, uint64_t propertyIdx) {
        return propertyColumns[relDirection].at(tableID)[propertyIdx].get();
    }
    inline ListsWithAdjAndPropertyListsUpdateStore* getPropertyLists(
        RelDirection relDirection, table_id_t tableID, uint64_t propertyIdx) {
        return propertyLists[relDirection].at(tableID)[propertyIdx].get();
    }
    inline AdjColumn* getAdjColumn(RelDirection relDirection, table_id_t tableID) {
        return adjColumns[relDirection].at(tableID).get();
    }
    inline AdjLists* getAdjLists(RelDirection relDirection, table_id_t tableID) {
        return adjLists[relDirection].at(tableID).get();
    }
    inline AdjAndPropertyListsUpdateStore* getAdjAndPropertyListsUpdateStore() {
        return adjAndPropertyListsUpdateStore.get();
    }

    vector<AdjLists*> getAdjListsForNodeTable(table_id_t tableID);
    vector<AdjColumn*> getAdjColumnsForNodeTable(table_id_t tableID);

    void prepareCommitOrRollbackIfNecessary(bool isCommit);
    void checkpointInMemoryIfNecessary();
    void rollbackInMemoryIfNecessary();
    void insertRels(vector<shared_ptr<ValueVector>>& valueVectorsToInsert);

private:
    inline void addToUpdatedRelTables() { wal->addToUpdatedRelTables(tableID); }
    inline void clearAdjAndPropertyListsUpdateStore() { adjAndPropertyListsUpdateStore->clear(); }
    void initAdjColumnOrLists(const catalog::Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerTable, BufferManager& bufferManager, WAL* wal);
    void initPropertyListsAndColumns(
        const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal);
    void initPropertyColumnsForRelTable(const catalog::Catalog& catalog, RelDirection relDirection,
        BufferManager& bufferManager, WAL* wal);
    void initPropertyListsForRelTable(const catalog::Catalog& catalog, RelDirection relDirection,
        BufferManager& bufferManager, WAL* wal);
    void performOpOnListsWithUpdates(std::function<void(Lists*)> opOnListsWithUpdates,
        std::function<void()> opIfHasInsertedRels);

private:
    shared_ptr<spdlog::logger> logger;
    table_id_t tableID;
    vector<table_property_columns_map_t> propertyColumns;
    vector<table_adj_columns_map_t> adjColumns;
    vector<table_property_lists_map_t> propertyLists;
    vector<table_adj_lists_map_t> adjLists;
    bool isInMemoryMode;
    unique_ptr<AdjAndPropertyListsUpdateStore> adjAndPropertyListsUpdateStore;
    WAL* wal;
};

} // namespace storage
} // namespace graphflow
