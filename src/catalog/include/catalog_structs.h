#pragma once

#include <unordered_map>
#include <unordered_set>

#include "src/common/include/configs.h"
#include "src/common/types/include/types_include.h"

using namespace graphflow::common;

namespace graphflow {
namespace catalog {

struct SrcDstLabels {
    SrcDstLabels(unordered_set<label_t> srcNodeLabels, unordered_set<label_t> dstNodeLabels)
        : srcNodeLabels{move(srcNodeLabels)}, dstNodeLabels{move(dstNodeLabels)} {}
    SrcDstLabels() = default;
    unordered_set<label_t> srcNodeLabels;
    unordered_set<label_t> dstNodeLabels;
};

enum RelMultiplicity : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };
RelMultiplicity getRelMultiplicityFromString(const string& relMultiplicityString);

// A PropertyNameDataType consists of its name, id, and dataType. If the property is unstructured,
// then the dataType's typeID is UNSTRUCTURED, otherwise it is one of those supported by the system.
struct PropertyNameDataType {

public:
    // This constructor is needed for ser/deser functions
    PropertyNameDataType(){};

    PropertyNameDataType(string name, DataTypeID dataTypeID)
        : PropertyNameDataType{move(name), DataType(dataTypeID)} {
        assert(dataTypeID != LIST);
    }

    PropertyNameDataType(string name, DataType dataType)
        : name{move(name)}, dataType{move(dataType)} {};

    inline bool isIDProperty() const { return name == LoaderConfig::ID_FIELD; }

public:
    string name;
    DataType dataType;
};

struct Property : PropertyNameDataType {

private:
    Property(string name, DataType dataType, uint32_t propertyID, label_t label, bool isNodeLabel)
        : PropertyNameDataType{name, dataType}, propertyID{propertyID}, label{label},
          isNodeLabel{isNodeLabel} {}

public:
    // This constructor is needed for ser/deser functions
    Property() {}

    static Property constructUnstructuredNodeProperty(
        string name, uint32_t propertyID, label_t nodeLabel) {
        return Property(
            name, DataType(UNSTRUCTURED), propertyID, nodeLabel, true /* is node label */);
    }

    static Property constructStructuredNodeProperty(
        PropertyNameDataType nameDataType, uint32_t propertyID, label_t nodeLabel) {
        return Property(nameDataType.name, nameDataType.dataType, propertyID, nodeLabel,
            true /* is node label */);
    }

    static inline Property constructRelProperty(
        PropertyNameDataType nameDataType, uint32_t propertyID, label_t relLabel) {
        return Property(nameDataType.name, nameDataType.dataType, propertyID, relLabel,
            false /* is node label */);
    }

public:
    uint32_t propertyID;
    label_t label;
    bool isNodeLabel;
};

struct Label {
protected:
    Label(string labelName, label_t labelId) : labelName{move(labelName)}, labelId{labelId} {}

public:
    string labelName;
    label_t labelId;
};

struct NodeLabel : Label {
    NodeLabel() : NodeLabel{"", UINT64_MAX, UINT64_MAX, vector<Property>{}} {}
    NodeLabel(string labelName, label_t labelId, uint64_t primaryPropertyId,
        vector<Property> structuredProperties)
        : Label{move(labelName), labelId}, primaryPropertyId{primaryPropertyId},
          structuredProperties{move(structuredProperties)} {}

    inline uint64_t getNumStructuredProperties() const { return structuredProperties.size(); }
    inline uint64_t getNumUnstructuredProperties() const { return unstructuredProperties.size(); }
    void addUnstructuredProperties(vector<string>& unstructuredPropertyNames);

    inline void addFwdRelLabel(label_t relLabelId) { fwdRelLabelIdSet.insert(relLabelId); }
    inline void addBwdRelLabel(label_t relLabelId) { bwdRelLabelIdSet.insert(relLabelId); }

    inline Property getPrimaryKey() const { return structuredProperties[primaryPropertyId]; }

    vector<Property> getAllNodeProperties() const;

    uint64_t primaryPropertyId;
    vector<Property> structuredProperties, unstructuredProperties;
    unordered_set<label_t> fwdRelLabelIdSet; // srcNode->rel
    unordered_set<label_t> bwdRelLabelIdSet; // dstNode->rel
    // This map is maintained as a cache for unstructured properties. It is not serialized to the
    // catalog file, but is re-constructed when reading from the catalog file.
    unordered_map<string, uint64_t> unstrPropertiesNameToIdMap;
};

struct RelLabel : Label {
    RelLabel()
        : Label{"", UINT64_MAX}, relMultiplicity{MANY_MANY}, numGeneratedProperties{UINT32_MAX} {}
    RelLabel(string labelName, label_t labelId, RelMultiplicity relMultiplicity,
        vector<Property> properties, SrcDstLabels srcDstLabels);

    inline void addRelIDDefinition() {
        auto propertyNameDataType = PropertyNameDataType(INTERNAL_ID_SUFFIX, INT64);
        properties.push_back(
            Property::constructRelProperty(propertyNameDataType, properties.size(), labelId));
        numGeneratedProperties++;
    }
    inline Property& getRelIDDefinition() {
        for (auto& property : properties) {
            if (property.name == INTERNAL_ID_SUFFIX) {
                return property;
            }
        }
        assert(false);
    }

    inline uint32_t getNumProperties() const { return properties.size(); }
    inline uint32_t getNumPropertiesToReadFromCSV() const {
        assert(properties.size() >= numGeneratedProperties);
        return properties.size() - numGeneratedProperties;
    }

    RelMultiplicity relMultiplicity;
    uint32_t numGeneratedProperties;
    vector<Property> properties;
    SrcDstLabels srcDstLabels;
    // Cardinality information
    vector<unordered_map<label_t, uint64_t>> numRelsPerDirectionBoundLabel;
};
} // namespace catalog
} // namespace graphflow
