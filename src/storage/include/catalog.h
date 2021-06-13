#pragma once

#include <memory>
#include <unordered_map>

#include "nlohmann/json.hpp"

#include "src/common/include/types.h"
#include "src/common/include/utils.h"

using namespace graphflow::common;
using namespace std;

namespace spdlog {
class logger;
}

namespace bitsery {
class Access;
}

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
    Catalog();
    Catalog(const string& directory);

    virtual ~Catalog() = default;

    inline const uint32_t getNodeLabelsCount() const { return stringToNodeLabelMap.size(); }
    inline const uint32_t getRelLabelsCount() const { return stringToRelLabelMap.size(); }

    virtual inline bool containNodeLabel(const char* label) const {
        return end(stringToNodeLabelMap) != stringToNodeLabelMap.find(label);
    }

    virtual inline label_t getNodeLabelFromString(const char* label) const {
        return stringToNodeLabelMap.at(label);
    }

    virtual inline bool containRelLabel(const char* label) const {
        return end(stringToRelLabelMap) != stringToRelLabelMap.find(label);
    }

    virtual inline label_t getRelLabelFromString(const char* label) const {
        return stringToRelLabelMap.at(label);
    }

    inline const unordered_map<string, PropertyKey>& getUnstrPropertyKeyMapForNodeLabel(
        const label_t& nodeLabel) const {
        return nodeUnstrPropertyKeyMaps[nodeLabel];
    }

    inline const unordered_map<string, PropertyKey>& getPropertyKeyMapForNodeLabel(
        label_t nodeLabel) const {
        return nodePropertyKeyMaps[nodeLabel];
    }

    inline const unordered_map<string, PropertyKey>& getPropertyKeyMapForRelLabel(
        label_t relLabel) const {
        return relPropertyKeyMaps[relLabel];
    }

    virtual inline bool containNodeProperty(label_t nodeLabel, const string& propertyName) const {
        auto& nodeProperties = getPropertyKeyMapForNodeLabel(nodeLabel);
        return end(nodeProperties) != nodeProperties.find(propertyName);
    }

    virtual inline bool containUnstrNodeProperty(
        label_t nodeLabel, const string& propertyName) const {
        auto& nodeProperties = getUnstrPropertyKeyMapForNodeLabel(nodeLabel);
        return end(nodeProperties) != nodeProperties.find(propertyName);
    }

    virtual inline DataType getNodePropertyTypeFromString(
        label_t nodeLabel, const string& propertyName) const {
        auto& nodeProperties = getPropertyKeyMapForNodeLabel(nodeLabel);
        return nodeProperties.at(propertyName).dataType;
    }

    inline uint32_t getUnstrNodePropertyKeyFromString(label_t nodeLabel, const string& name) const {
        return getUnstrPropertyKeyMapForNodeLabel(nodeLabel).at(name).idx;
    }

    virtual inline uint32_t getNodePropertyKeyFromString(
        label_t nodeLabel, const string& name) const {
        return getPropertyKeyMapForNodeLabel(nodeLabel).at(name).idx;
    }

    virtual inline bool containRelProperty(label_t relLabel, const string& propertyName) const {
        auto relProperties = getPropertyKeyMapForRelLabel(relLabel);
        return end(relProperties) != relProperties.find(propertyName);
    }

    virtual inline DataType getRelPropertyTypeFromString(
        label_t relLabel, const string& propertyName) const {
        auto& relProperties = getPropertyKeyMapForRelLabel(relLabel);
        return relProperties.at(propertyName).dataType;
    }

    virtual inline uint32_t getRelPropertyKeyFromString(
        label_t relLabel, const string& name) const {
        return getPropertyKeyMapForRelLabel(relLabel).at(name).idx;
    }

    const string getStringNodeLabel(label_t label) const;

    virtual const vector<label_t>& getRelLabelsForNodeLabelDirection(
        label_t nodeLabel, Direction dir) const;

    const vector<label_t>& getNodeLabelsForRelLabelDir(label_t relLabel, Direction dir) const;

    virtual bool isSingleCaridinalityInDir(label_t relLabel, Direction dir) const;

    unique_ptr<nlohmann::json> debugInfo();

private:
    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& directory);
    void readFromFile(const string& directory);

    void serializeStringToLabelMap(fstream& f, stringToLabelMap_t& map);
    void deserializeStringToLabelMap(fstream& f, stringToLabelMap_t& map);

    string getNodeLabelsString(vector<label_t> nodeLabels);

    unique_ptr<nlohmann::json> getPropertiesJson(
        const unordered_map<string, PropertyKey>& properties);

private:
    shared_ptr<spdlog::logger> logger;
    stringToLabelMap_t stringToNodeLabelMap, stringToRelLabelMap;
    // A prpertyKeyMap of a node or rel label maps the name of a structured property of that label
    // to a PropertyKey object that comprises of an index value (called propertyKeyIdx) and the
    // property's dataType. Hence, {(node/rel) label, propertyKeyIdx} pair uniquely represents a
    // structured property in the system.
    vector<unordered_map<string, PropertyKey>> nodePropertyKeyMaps, relPropertyKeyMaps;
    // nodeUnstrPrpertyKeyMap is a propertyKeyMap for a node label's unstructured properties. Each
    // entry of the map has the UNKNOWN dataType which means that the dataType of value of such a
    // property is not fixed and hence can vary from node to node.
    vector<unordered_map<string, PropertyKey>> nodeUnstrPropertyKeyMaps;
    vector<vector<label_t>> relLabelToSrcNodeLabels, relLabelToDstNodeLabels;
    vector<vector<label_t>> srcNodeLabelToRelLabel, dstNodeLabelToRelLabel;
    vector<Cardinality> relLabelToCardinalityMap;
};

} // namespace storage
} // namespace graphflow
