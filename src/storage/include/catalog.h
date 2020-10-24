#ifndef GRAPHFLOW_STORAGE_CATALOG_H_
#define GRAPHFLOW_STORAGE_CATALOG_H_

#include <memory>
#include <unordered_map>
#include <vector>

#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace common {

class GraphLoader;

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace storage {

class Catalog {

    friend class graphflow::common::GraphLoader;

public:
    inline uint32_t getNodeLabelsCount() { return stringToNodeLabelMap.size(); }
    inline uint32_t getRelLabelsCount() { return stringToRelLabelMap.size(); }
    inline const vector<vector<Property>> &getNodePropertyMap() { return nodePropertyMap; }

    inline gfLabel_t getNodeLabelForString(const string &nodeLabelString) {
        if (stringToNodeLabelMap.find(nodeLabelString) == stringToNodeLabelMap.end()) {
            throw invalid_argument("Node label: " + nodeLabelString + " not indexed in Catalog.");
        }
        return stringToNodeLabelMap[nodeLabelString];
    }

    inline gfLabel_t getRelLabelForString(const string &relLabelString) {
        if (stringToRelLabelMap.find(relLabelString) == stringToRelLabelMap.end()) {
            throw invalid_argument("Rel label: " + relLabelString + " not indexed in Catalog.");
        }
        return stringToRelLabelMap[relLabelString];
    }

private:
    Catalog(){};

private:
    unordered_map<string, gfLabel_t> stringToNodeLabelMap;
    unordered_map<string, gfLabel_t> stringToRelLabelMap;
    vector<vector<Property>> nodePropertyMap;
    vector<vector<Property>> relPropertyMap;
    vector<vector<gfLabel_t>> relLabelToSrcNodeLabel, relLabelToDstNodeLabel;
    vector<vector<gfLabel_t>> srcNodeLabelToRelLabel, dstNodeLabelToRelLabel;
    vector<Cardinality> relLabelToCardinalityMap;
};

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_CATALOG_H_
