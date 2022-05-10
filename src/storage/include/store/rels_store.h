#pragma once

#include "src/catalog/include/catalog.h"
#include "src/common/include/file_utils.h"
#include "src/storage/include/store/rel.h"

using namespace graphflow::common;
using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

// RelsStore stores adjacent rels of a node as well as the properties of rels in the system.
class RelsStore {

public:
    RelsStore(const Catalog& catalog, BufferManager& bufferManager, const string& directory,
        bool isInMemoryMode, WAL* wal);

    inline Column* getRelPropertyColumn(
        const label_t& relLabel, const label_t& nodeLabel, const uint64_t& propertyIdx) const {
        return rels[relLabel]->getPropertyColumn(nodeLabel, propertyIdx);
    }
    inline Lists* getRelPropertyLists(const RelDirection& relDirection, const label_t& nodeLabel,
        const label_t& relLabel, const uint64_t& propertyIdx) const {
        return rels[relLabel]->getPropertyLists(relDirection, nodeLabel, propertyIdx);
    }
    inline AdjColumn* getAdjColumn(
        const RelDirection& relDirection, const label_t& nodeLabel, const label_t& relLabel) const {
        return rels[relLabel]->getAdjColumn(relDirection, nodeLabel);
    }
    inline AdjLists* getAdjLists(
        const RelDirection& relDirection, const label_t& nodeLabel, const label_t& relLabel) const {
        return rels[relLabel]->getAdjLists(relDirection, nodeLabel);
    }

private:
    shared_ptr<spdlog::logger> logger;
    vector<unique_ptr<Rel>> rels;
};

} // namespace storage
} // namespace graphflow
