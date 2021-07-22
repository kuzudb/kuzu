#pragma once

#include <memory>

#include "nlohmann/json.hpp"

#include "src/common/include/assert.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/function.h"
#include "src/common/include/ser_deser.h"
#include "src/common/include/types.h"
#include "src/common/include/utils.h"

using namespace graphflow::common;
using namespace std;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

enum RelMultiplicity : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };

const string RelMultiplicityNames[] = {"MANY_MANY", "MANY_ONE", "ONE_MANY", "ONE_ONE"};

RelMultiplicity getRelMultiplicity(const string& relMultiplicityString);

// A PropertyDefinition consists of its name, dataType, id and isPrimaryKey. If the property is
// unstructured, then the dataType is UNSTRUCTURED, otherwise it is one of those supported by the
// system.
struct PropertyDefinition {

public:
    PropertyDefinition() : id{0}, dataType{0}, isPrimaryKey{false} {};

    PropertyDefinition(string name, uint32_t id, DataType dataType)
        : name{move(name)}, id{id}, dataType{dataType}, isPrimaryKey{false} {};

public:
    string name;
    uint32_t id;
    DataType dataType;
    bool isPrimaryKey;
};

struct LabelDefinition {
protected:
    LabelDefinition(string labelName, label_t labelId)
        : labelName{move(labelName)}, labelId{labelId} {}

public:
    string labelName;
    label_t labelId;
};

struct NodeLabelDefinition : LabelDefinition {
    NodeLabelDefinition() : LabelDefinition{"", 0}, primaryPropertyId{0} {}
    NodeLabelDefinition(string labelName, label_t labelId, uint64_t primaryPropertyId,
        vector<PropertyDefinition> structuredProperties)
        : LabelDefinition{move(labelName), labelId}, primaryPropertyId{primaryPropertyId},
          structuredProperties{move(structuredProperties)} {}

    uint64_t primaryPropertyId;
    vector<PropertyDefinition> structuredProperties, unstructuredProperties;
    unordered_set<label_t> fwdRelLabelIdSet; // srcNode->rel
    unordered_set<label_t> bwdRelLabelIdSet; // dstNode->rel
    // This map is maintained as a cache for unstructured properties. It is not serialized to the
    // catalog file, but is re-constructed when reading from the catalog file.
    unordered_map<string, uint64_t> unstrPropertiesNameToIdMap;
};

struct RelLabelDefinition : LabelDefinition {
    RelLabelDefinition() : LabelDefinition{"", 0}, relMultiplicity{0} {}
    RelLabelDefinition(string labelName, label_t labelId, RelMultiplicity relMultiplicity,
        vector<PropertyDefinition> properties, unordered_set<label_t> srcNodeLabelIdSet,
        unordered_set<label_t> dstNodeLabelIdSet)
        : LabelDefinition{move(labelName), labelId}, relMultiplicity{relMultiplicity},
          properties{move(properties)}, srcNodeLabelIdSet{move(srcNodeLabelIdSet)},
          dstNodeLabelIdSet{move(dstNodeLabelIdSet)} {}

    RelMultiplicity relMultiplicity;
    vector<PropertyDefinition> properties;
    unordered_set<label_t> srcNodeLabelIdSet;
    unordered_set<label_t> dstNodeLabelIdSet;
};

class Catalog {

public:
    Catalog();
    explicit Catalog(const string& directory);

    virtual ~Catalog() = default;

    /**
     * Node and Rel label functions.
     */

    void addNodeLabel(string labelName, vector<PropertyDefinition> structuredProperties,
        const string& primaryKeyPropertyName);

    void addRelLabel(string labelName, RelMultiplicity relMultiplicity,
        vector<PropertyDefinition> properties, const vector<string>& srcNodeLabelNames,
        const vector<string>& dstNodeLabelNames);

    virtual inline string getNodeLabelName(label_t labelId) const {
        return nodeLabels[labelId].labelName;
    }
    virtual inline string getRelLabelName(label_t labelId) const {
        return relLabels[labelId].labelName;
    }

    inline uint64_t getNodeLabelsCount() const { return nodeLabels.size(); }
    inline uint64_t getRelLabelsCount() const { return relLabels.size(); }

    virtual inline bool containNodeLabel(const string& label) const {
        return end(nodeLabelNameToIdMap) != nodeLabelNameToIdMap.find(label);
    }
    virtual inline bool containRelLabel(const string& label) const {
        return end(relLabelNameToIdMap) != relLabelNameToIdMap.find(label);
    }

    virtual inline label_t getNodeLabelFromString(const string& label) const {
        return nodeLabelNameToIdMap.at(label);
    }
    virtual inline label_t getRelLabelFromString(const string& label) const {
        return relLabelNameToIdMap.at(label);
    }

    /**
     * Node and Rel property functions.
     */

    void addNodeUnstrProperty(uint64_t labelId, const string& propertyName);

    virtual bool containNodeProperty(label_t labelId, const string& propertyName) const;
    virtual bool containRelProperty(label_t relLabel, const string& propertyName) const;

    // getNodeProperty and getRelProperty should be called after checking if property exists
    // (containNodeProperty and containRelProperty).
    virtual const PropertyDefinition& getNodeProperty(
        label_t labelId, const string& propertyName) const;
    virtual const PropertyDefinition& getRelProperty(
        label_t labelId, const string& propertyName) const;

    const vector<PropertyDefinition> getAllNodeProperties(label_t nodeLabel) const;
    inline const vector<PropertyDefinition>& getStructuredNodeProperties(label_t nodeLabel) const {
        return nodeLabels[nodeLabel].structuredProperties;
    }
    inline const vector<PropertyDefinition>& getUnstructuredNodeProperties(
        label_t nodeLabel) const {
        return nodeLabels[nodeLabel].unstructuredProperties;
    }
    inline const vector<PropertyDefinition>& getRelProperties(label_t relLabel) const {
        return relLabels[relLabel].properties;
    }

    inline const unordered_map<string, uint64_t>& getUnstrPropertiesNameToIdMap(
        label_t nodeLabel) const {
        return nodeLabels[nodeLabel].unstrPropertiesNameToIdMap;
    }

    /**
     * Graph topology functions.
     */

    const unordered_set<label_t>& getNodeLabelsForRelLabelDirection(
        label_t relLabel, Direction direction) const;
    virtual const unordered_set<label_t>& getRelLabelsForNodeLabelDirection(
        label_t nodeLabel, Direction direction) const;

    virtual bool isSingleMultiplicityInDirection(label_t relLabel, Direction direction) const;

    // getFunction() should always be called after containFunction()
    inline bool containFunction(const string& functionName) const {
        return builtInFunctions.contains(functionName);
    }
    inline Function* getFunction(const string& functionName) const {
        return builtInFunctions.at(functionName).get();
    }

    unique_ptr<nlohmann::json> debugInfo();
    void saveToFile(const string& directory);
    void readFromFile(const string& directory);

private:
    string getNodeLabelsString(const unordered_set<label_t>& nodeLabels) const;

    void registerBuiltInFunctions();

private:
    shared_ptr<spdlog::logger> logger;
    vector<NodeLabelDefinition> nodeLabels;
    vector<RelLabelDefinition> relLabels;

    // These three maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    unordered_map<string, label_t> nodeLabelNameToIdMap;
    unordered_map<string, label_t> relLabelNameToIdMap;
    unordered_map<string, unique_ptr<Function>> builtInFunctions;
};

} // namespace storage
} // namespace graphflow
