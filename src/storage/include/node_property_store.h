#pragma once

#include <memory>
#include <vector>

#include "src/common/include/types.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/column.h"

namespace graphflow {
namespace storage {

class NodePropertyStore {

public:
    NodePropertyStore(Catalog& catalog, vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager);

    inline static string getNodePropertyColumnFname(
        const string& directory, gfLabel_t nodeLabel, const string& propertyName) {
        return directory + "/v-" + to_string(nodeLabel) + "-" + propertyName + ".vcol";
    }

private:
    vector<vector<unique_ptr<BaseColumn>>> columns;
};

} // namespace storage
} // namespace graphflow
