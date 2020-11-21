#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "bitsery/bitsery.h"

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

    inline uint32_t getNodeLabelsCount() const { return stringToNodeLabelMap.size(); }
    inline uint32_t getRelLabelsCount() const { return stringToRelLabelMap.size(); }

    inline const gfLabel_t &getNodeLabelFromString(string &label) const {
        return stringToNodeLabelMap.at(label);
    }
    inline const gfLabel_t &getRelLabelFromString(string &label) const {
        return stringToRelLabelMap.at(label);
    }

    inline const vector<Property> &getPropertyMapForNodeLabel(gfLabel_t nodeLabel) const {
        return nodePropertyMaps[nodeLabel];
    }
    inline const vector<Property> &getPropertyMapForRelLabel(gfLabel_t relLabel) const {
        return relPropertyMaps[relLabel];
    }

    const vector<gfLabel_t> &getRelLabelsForNodeLabelDirection(
        gfLabel_t nodeLabel, Direction direction) const;
    const vector<gfLabel_t> &getNodeLabelsForRelLabelDir(
        gfLabel_t relLabel, Direction direction) const;

    inline bool isSingleCaridinalityInDir(gfLabel_t relLabel, Direction direction) const {
        auto cardinality = relLabelToCardinalityMap[relLabel];
        if (FORWARD == direction) {
            return ONE_ONE == cardinality || MANY_ONE == cardinality;
        } else {
            return ONE_ONE == cardinality || ONE_MANY == cardinality;
        }
    }

private:
    Catalog() = default;

    template<typename S>
    void serialize(S &s);

    void saveToFile(const string &directory);
    void readFromFile(const string &directory);

private:
    unordered_map<string, gfLabel_t> stringToNodeLabelMap;
    unordered_map<string, gfLabel_t> stringToRelLabelMap;
    vector<vector<Property>> nodePropertyMaps;
    vector<vector<Property>> relPropertyMaps;
    vector<vector<gfLabel_t>> relLabelToSrcNodeLabels, relLabelToDstNodeLabels;
    vector<vector<gfLabel_t>> srcNodeLabelToRelLabel, dstNodeLabelToRelLabel;
    vector<Cardinality> relLabelToCardinalityMap;
};

} // namespace storage
} // namespace graphflow
