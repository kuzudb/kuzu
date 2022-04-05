#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/utils.h"
#include "src/storage/include/storage_structure/column.h"
#include "src/storage/include/storage_structure/lists/lists.h"
#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

using directional_adj_columns_t = unordered_map<label_t, unique_ptr<AdjColumn>>;
using directional_property_lists_t = unordered_map<label_t, vector<unique_ptr<Lists>>>;
using directional_adj_lists_t = unordered_map<label_t, unique_ptr<AdjLists>>;

class Rel {
public:
    explicit Rel(const catalog::Catalog& catalog, label_t relLabel, const string& directory,
        BufferManager& bufferManager, bool isInMemoryMode);

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

private:
    void initAdjColumnOrLists(const catalog::Catalog& catalog, const string& directory,
        BufferManager& bufferManager, bool isInMemoryMode);
    void initPropertyListsAndColumns(const catalog::Catalog& catalog, const string& directory,
        BufferManager& bufferManager, bool isInMemoryMode);
    void initPropertyColumnsForRelLabel(const catalog::Catalog& catalog, const string& directory,
        BufferManager& bufferManager, const RelDirection& dir, bool isInMemoryMode);
    void initPropertyListsForRelLabel(const catalog::Catalog& catalog, const string& directory,
        BufferManager& bufferManager, bool isInMemoryMode);

private:
    shared_ptr<spdlog::logger> logger;
    label_t relLabel;
    unordered_map<label_t, vector<unique_ptr<Column>>> propertyColumns;
    vector<directional_adj_columns_t> adjColumns{2};
    vector<directional_property_lists_t> propertyLists{2};
    vector<directional_adj_lists_t> adjLists{2};
};

} // namespace storage
} // namespace graphflow
