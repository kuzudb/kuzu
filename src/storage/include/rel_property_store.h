#pragma once

#include "src/common/include/types.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/column.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class RelPropertyStore {

public:
    RelPropertyStore(Catalog& catalog, vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager);

    inline static string getRelPropertyColumnFname(const string& directory, gfLabel_t relLabel,
        gfLabel_t nodeLabel, const string& propertyName) {
        return directory + "/e-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               propertyName + ".vcol";
    }

private:
    void initPropertyColumnsForRelLabel(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager, gfLabel_t relLabel, Direction dir);

private:
    vector<vector<unique_ptr<BaseColumn>>> propertyColumns;
};

} // namespace storage
} // namespace graphflow
