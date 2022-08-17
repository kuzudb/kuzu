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
        BufferManager& bufferManager, bool isInMemoryMode, WAL* wal);

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

    pair<vector<AdjLists*>, vector<AdjColumn*>> getAdjListsAndColumns(
        const label_t nodeLabel) const;

    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of relTables. We just need to add the actual relTable to
    // relStore when checkpointing and not in recovery mode. In other words, this function should
    // only be called by wal_replayer during checkpointing, during which time no other transaction
    // is running on the system, so we can directly create and insert a RelTable into relTables.
    inline void createRelTable(label_t labelID, vector<uint64_t>& maxNodeOffsetsPerLabel,
        BufferManager* bufferManager, WAL* wal, Catalog* catalog) {
        relTables.push_back(make_unique<RelTable>(
            *catalog, maxNodeOffsetsPerLabel, labelID, *bufferManager, isInMemoryMode, wal));
    }

    // This function is used for testing only.
    inline uint32_t getNumRelTables() const { return relTables.size(); }

    inline RelTable* getRel(label_t labelID) const { return relTables[labelID].get(); }

private:
    vector<unique_ptr<RelTable>> relTables;
    bool isInMemoryMode;
};

} // namespace storage
} // namespace graphflow
