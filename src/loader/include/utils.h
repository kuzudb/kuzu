#pragma once

#include <memory>

#include "robin_hood.h"

#include "src/common/include/configs.h"
#include "src/common/include/types.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

constexpr char EMPTY_STRING = 0;

//! NodeIDMap maps the primary key of a node to the in-system used node offset.
class NodeIDMap {

public:
    NodeIDMap(const uint64_t& size) : size{size}, offsetToNodeIDMap(make_unique<char*[]>(size)){};
    ~NodeIDMap();

    void set(const char* nodeID, node_offset_t nodeOffset);

    node_offset_t get(const char* nodeID);

    inline void createNodeIDToOffsetMap() {
        nodeIDToOffsetMap.reserve(1.5 * size);
        for (auto i = 0u; i < size; i++) {
            nodeIDToOffsetMap.emplace(offsetToNodeIDMap[i], i);
        }
    }

private:
    uint64_t size;
    robin_hood::unordered_flat_map<const char*, node_offset_t, charArrayHasher, charArrayEqualTo>
        nodeIDToOffsetMap;
    unique_ptr<char*[]> offsetToNodeIDMap;
};

vector<DataType> createPropertyDataTypes(const unordered_map<string, Property>& propertyMap);

//! Holds information about a rel label that is needed to construct adjRels and adjLists
//! indexes, property columns, and property lists.
class RelLabelDescription {

public:
    bool hasProperties() { return propertyMap->size() > 0; }

    bool requirePropertyLists() {
        return hasProperties() && !isSingleCardinalityPerDir[FWD] &&
               !isSingleCardinalityPerDir[BWD];
    };

    uint32_t getRelSize(Direction dir) {
        return numBytesSchemePerDir[dir].first + numBytesSchemePerDir[dir].second;
    }

public:
    label_t label;
    string fname;
    uint64_t numBlocks;
    vector<vector<label_t>> nodeLabelsPerDir{2};
    vector<bool> isSingleCardinalityPerDir{false, false};
    vector<pair<uint32_t, uint32_t>> numBytesSchemePerDir{2};
    const unordered_map<string, Property>* propertyMap;
    vector<DataType> propertyDataTypes;
};

} // namespace loader
} // namespace graphflow
