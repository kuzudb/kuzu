#ifndef GRAPHFLOW_STORAGE_CATALOG_H_
#define GRAPHFLOW_STORAGE_CATALOG_H_

#include <memory>
#include <unordered_map>
#include <vector>

#include "bitsery/bitsery.h"
#include "nlohmann/json.hpp"

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
    friend class bitsery::Access;

public:
    Catalog(const string &directory) { readFromFile(directory); };

    inline uint32_t getNodeLabelsCount() { return stringToNodeLabelMap.size(); }
    inline uint32_t getRelLabelsCount() { return stringToRelLabelMap.size(); }

    inline const vector<Property> &getNodePropertyMap(gfLabel_t nodeLabel) {
        return nodePropertyMap[nodeLabel];
    }

    const vector<gfLabel_t> &getRelLabelsForNodeLabelDirection(
        gfLabel_t nodeLabel, Direction direction);
    const vector<gfLabel_t> &getNodeLabelsForRelLabelDir(
        gfLabel_t relLabel, Direction direction);

    inline bool isSingleCaridinalityInDir(gfLabel_t relLabel, Direction direction) {
        auto cardinality = relLabelToCardinalityMap[relLabel];
        if (FORWARD == direction) {
            return ONE_ONE == cardinality || MANY_ONE == cardinality;
        } else {
            return ONE_ONE == cardinality || ONE_MANY == cardinality;
        }
    }

private:
    Catalog(const nlohmann::json &metadata);

    void assignLabels(
        unordered_map<string, gfLabel_t> &stringToLabelMap, const nlohmann::json &fileDescriptions);
    void setCardinalities(const nlohmann::json &metadata);
    void setSrcDstNodeLabelsForRelLabels(const nlohmann::json &metadata);

    template<typename S>
    void serialize(S &s);

    void saveToFile(const string &directory);
    void readFromFile(const string &directory);

private:
    unordered_map<string, gfLabel_t> stringToNodeLabelMap;
    unordered_map<string, gfLabel_t> stringToRelLabelMap;
    vector<vector<Property>> nodePropertyMap;
    vector<vector<Property>> relPropertyMap;
    vector<vector<gfLabel_t>> relLabelToSrcNodeLabels, relLabelToDstNodeLabels;
    vector<vector<gfLabel_t>> srcNodeLabelToRelLabel, dstNodeLabelToRelLabel;
    vector<Cardinality> relLabelToCardinalityMap;
};

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_CATALOG_H_
