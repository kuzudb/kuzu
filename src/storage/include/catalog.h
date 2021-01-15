#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "bitsery/bitsery.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace loader {

class GraphLoader;

} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

typedef unordered_map<const char*, label_t, charArrayHasher, charArrayEqualTo> stringToLabelMap_t;

class Catalog {
    friend class graphflow::loader::GraphLoader;
    friend class bitsery::Access;

public:
    Catalog(const string& directory);

    inline const uint32_t getNodeLabelsCount() const { return stringToNodeLabelMap.size(); }
    inline const uint32_t getRelLabelsCount() const { return stringToRelLabelMap.size(); }

    inline const label_t& getNodeLabelFromString(const char* label) const {
        return stringToNodeLabelMap.at(label);
    }
    inline const label_t& getRelLabelFromString(const char* label) const {
        return stringToRelLabelMap.at(label);
    }

    inline const unordered_map<string, Property>& getPropertyMapForNodeLabel(
        const label_t& nodeLabel) const {
        return nodePropertyMaps[nodeLabel];
    }
    inline const unordered_map<string, Property>& getPropertyMapForRelLabel(
        const label_t& relLabel) const {
        return relPropertyMaps[relLabel];
    }

    inline const uint32_t& getNodePropertyKeyFromString(
        const label_t& nodeLabel, const string& name) const {
        return getPropertyMapForNodeLabel(nodeLabel).at(name).idx;
    }
    inline const uint32_t& getRelPropertyKeyFromString(
        const label_t& relLabel, const string& name) const {
        return getPropertyMapForRelLabel(relLabel).at(name).idx;
    }

    const vector<label_t>& getRelLabelsForNodeLabelDirection(
        const label_t& nodeLabel, const Direction& dir) const;
    const vector<label_t>& getNodeLabelsForRelLabelDir(
        const label_t& relLabel, const Direction& dir) const;

    bool isSingleCaridinalityInDir(const label_t& relLabel, const Direction& dir) const;

private:
    Catalog() : logger{spdlog::get("storage")} {};

    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& directory);
    void readFromFile(const string& directory);

    void serializeStringToLabelMap(fstream& f, stringToLabelMap_t& map);
    void deserializeStringToLabelMap(fstream& f, stringToLabelMap_t& map);

private:
    shared_ptr<spdlog::logger> logger;
    stringToLabelMap_t stringToNodeLabelMap;
    stringToLabelMap_t stringToRelLabelMap;
    vector<unordered_map<string, Property>> nodePropertyMaps;
    vector<unordered_map<string, Property>> relPropertyMaps;
    vector<vector<label_t>> relLabelToSrcNodeLabels, relLabelToDstNodeLabels;
    vector<vector<label_t>> srcNodeLabelToRelLabel, dstNodeLabelToRelLabel;
    vector<Cardinality> relLabelToCardinalityMap;
};

} // namespace storage
} // namespace graphflow
