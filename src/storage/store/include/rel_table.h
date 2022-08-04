#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/utils.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/storage_structure/include/column.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace storage {

using label_adj_columns_map_t = unordered_map<label_t, unique_ptr<AdjColumn>>;
using label_property_lists_map_t = unordered_map<label_t, vector<unique_ptr<Lists>>>;
using label_adj_lists_map_t = unordered_map<label_t, unique_ptr<AdjLists>>;

class RelTable {

public:
    explicit RelTable(const catalog::Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerLabel, label_t relLabel,
        BufferManager& bufferManager, bool isInMemoryMode, WAL* wal);

public:
    inline Column* getPropertyColumn(label_t nodeLabel, uint64_t propertyIdx) {
        return propertyColumns.at(nodeLabel)[propertyIdx].get();
    }
    inline Lists* getPropertyLists(
        RelDirection relDirection, label_t nodeLabel, uint64_t propertyIdx) {
        return propertyLists[relDirection].at(nodeLabel)[propertyIdx].get();
    }
    inline AdjColumn* getAdjColumn(RelDirection relDirection, label_t nodeLabel) {
        return adjColumns[relDirection].at(nodeLabel).get();
    }
    inline AdjLists* getAdjLists(RelDirection relDirection, label_t nodeLabel) {
        return adjLists[relDirection].at(nodeLabel).get();
    }

    vector<AdjLists*> getAdjListsForNodeLabel(label_t nodeLabel);
    vector<AdjColumn*> getAdjColumnsForNodeLabel(label_t nodeLabel);

private:
    void initAdjColumnOrLists(const catalog::Catalog& catalog,
        const vector<uint64_t>& maxNodeOffsetsPerLabel, const string& directory,
        BufferManager& bufferManager, bool isInMemoryMode, WAL* wal);
    void initPropertyListsAndColumns(const catalog::Catalog& catalog, const string& directory,
        BufferManager& bufferManager, bool isInMemoryMode, WAL* wal);
    void initPropertyColumnsForRelLabel(const catalog::Catalog& catalog, const string& directory,
        BufferManager& bufferManager, RelDirection relDirection, bool isInMemoryMode, WAL* wal);
    void initPropertyListsForRelLabel(const catalog::Catalog& catalog, const string& directory,
        BufferManager& bufferManager, RelDirection relDirection, bool isInMemoryMode, WAL* wal);

private:
    shared_ptr<spdlog::logger> logger;
    label_t relLabel;
    unordered_map<label_t, vector<unique_ptr<Column>>> propertyColumns;
    vector<label_adj_columns_map_t> adjColumns{2};
    vector<label_property_lists_map_t> propertyLists{2};
    vector<label_adj_lists_map_t> adjLists{2};
};

} // namespace storage
} // namespace graphflow
