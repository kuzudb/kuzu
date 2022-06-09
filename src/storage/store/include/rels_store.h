#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/file_utils.h"
#include "src/storage/store/include/rel_table.h"

using namespace graphflow::common;
using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

// RelsStore stores adjacent rels of a node as well as the properties of rels in the system.
class RelsStore {

public:
    RelsStore(const Catalog& catalog, const vector<uint64_t>& maxNodeOffsetsPerLabel,
        BufferManager& bufferManager, const string& directory, bool isInMemoryMode, WAL* wal);

    inline Column* getRelPropertyColumn(
        const label_t& relLabel, const label_t& nodeLabel, const uint64_t& propertyIdx) const {
        return relTables[relLabel]->getPropertyColumn(nodeLabel, propertyIdx);
    }
    inline Lists* getRelPropertyLists(const RelDirection& relDirection, const label_t& nodeLabel,
        const label_t& relLabel, const uint64_t& propertyIdx) const {
        return relTables[relLabel]->getPropertyLists(relDirection, nodeLabel, propertyIdx);
    }
    inline AdjColumn* getAdjColumn(
        const RelDirection& relDirection, const label_t& nodeLabel, const label_t& relLabel) const {
        return relTables[relLabel]->getAdjColumn(relDirection, nodeLabel);
    }
    inline AdjLists* getAdjLists(
        const RelDirection& relDirection, const label_t& nodeLabel, const label_t& relLabel) const {
        return relTables[relLabel]->getAdjLists(relDirection, nodeLabel);
    }

    inline pair<vector<AdjLists*>, vector<AdjColumn*>> getAdjListsAndColumns(
        const label_t nodeLabel) const {
        vector<AdjLists*> adjListsRetVal;
        for (uint64_t i = 0; i < relTables.size(); ++i) {
            auto adjListsForRel = relTables[i]->getAdjListsForNodeLabel(nodeLabel);
            adjListsRetVal.insert(
                adjListsRetVal.end(), adjListsForRel.begin(), adjListsForRel.end());
        }
        vector<AdjColumn*> adjColumnsRetVal;
        for (uint64_t i = 0; i < relTables.size(); ++i) {
            auto adjColumnsForRel = relTables[i]->getAdjColumnsForNodeLabel(nodeLabel);
            adjColumnsRetVal.insert(
                adjColumnsRetVal.end(), adjColumnsForRel.begin(), adjColumnsForRel.end());
        }
        return make_pair(adjListsRetVal, adjColumnsRetVal);
    }

private:
    vector<unique_ptr<RelTable>> relTables;
};

} // namespace storage
} // namespace graphflow
