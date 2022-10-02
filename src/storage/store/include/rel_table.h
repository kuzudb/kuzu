#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/utils.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/storage_structure/include/column.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace storage {

using table_adj_columns_map_t = unordered_map<table_id_t, unique_ptr<AdjColumn>>;
using table_property_lists_map_t = unordered_map<table_id_t, vector<unique_ptr<Lists>>>;
using table_adj_lists_map_t = unordered_map<table_id_t, unique_ptr<AdjLists>>;

class RelTable {

public:
    explicit RelTable(const catalog::Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerTable, table_id_t tableID,
        BufferManager& bufferManager, MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal);

    void loadColumnsAndListsFromDisk(const catalog::Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerTable, BufferManager& bufferManager, WAL* wal);

public:
    inline Column* getPropertyColumn(table_id_t tableID, uint64_t propertyIdx) {
        return propertyColumns.at(tableID)[propertyIdx].get();
    }
    inline Lists* getPropertyLists(
        RelDirection relDirection, table_id_t tableID, uint64_t propertyIdx) {
        return propertyLists[relDirection].at(tableID)[propertyIdx].get();
    }
    inline AdjColumn* getAdjColumn(RelDirection relDirection, table_id_t tableID) {
        return adjColumns[relDirection].at(tableID).get();
    }
    inline AdjLists* getAdjLists(RelDirection relDirection, table_id_t tableID) {
        return adjLists[relDirection].at(tableID).get();
    }

    vector<AdjLists*> getAdjListsForNodeTable(table_id_t tableID);
    vector<AdjColumn*> getAdjColumnsForNodeTable(table_id_t tableID);

    inline RelUpdateStore* getRelUpdateStore() { return relUpdateStore.get(); }
    void prepareCommitOrRollbackIfNecessary(bool isCommit);

private:
    void initAdjColumnOrLists(const catalog::Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerTable, BufferManager& bufferManager, WAL* wal);
    void initPropertyListsAndColumns(
        const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal);
    void initPropertyColumnsForRelTable(const catalog::Catalog& catalog, RelDirection relDirection,
        BufferManager& bufferManager, WAL* wal);
    void initPropertyListsForRelTable(const catalog::Catalog& catalog, RelDirection relDirection,
        BufferManager& bufferManager, WAL* wal);

private:
    shared_ptr<spdlog::logger> logger;
    table_id_t tableID;
    unordered_map<table_id_t, vector<unique_ptr<Column>>> propertyColumns;
    vector<table_adj_columns_map_t> adjColumns;
    vector<table_property_lists_map_t> propertyLists;
    vector<table_adj_lists_map_t> adjLists;
    bool isInMemoryMode;
    unique_ptr<RelUpdateStore> relUpdateStore;
};

} // namespace storage
} // namespace graphflow
