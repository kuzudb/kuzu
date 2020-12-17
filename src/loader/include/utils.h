#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "src/common/include/configs.h"
#include "src/common/include/types.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

//! NodeIDMap maps the primary key of a node to the in-system used node offset.
class NodeIDMap {

public:
    NodeIDMap(uint64_t size) : nodeIDToOffsetMapping{size} {};
    ~NodeIDMap();

    void setOffset(const char* nodeID, node_offset_t offset);
    node_offset_t getOffset(const char* nodeID);
    void merge(NodeIDMap& localMap);

private:
    unordered_map<const char*, node_offset_t, charArrayHasher, charArrayEqualTo>
        nodeIDToOffsetMapping;
};

//! Holds information about a rel label that is needed to construct adjRels and adjLists indexes,
//! property columns, and property lists.
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
    const vector<Property>* propertyMap;
};

} // namespace loader
} // namespace graphflow
