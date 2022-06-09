#pragma once

#include <memory>
#include <utility>

#include "catalog_structs.h"
#include "nlohmann/json.hpp"

#include "src/common/include/assert.h"
#include "src/common/include/exception.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/ser_deser.h"
#include "src/common/include/utils.h"
#include "src/function/aggregate/include/built_in_aggregate_functions.h"
#include "src/function/include/built_in_vector_operations.h"

using namespace graphflow::common;
using namespace graphflow::function;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace catalog {

class Catalog {

public:
    Catalog();
    explicit Catalog(const string& directory);

    virtual ~Catalog() = default;

    inline BuiltInVectorOperations* getBuiltInScalarFunctions() const {
        return builtInVectorOperations.get();
    }

    inline BuiltInAggregateFunctions* getBuiltInAggregateFunction() const {
        return builtInAggregateFunctions.get();
    }

    inline ExpressionType getFunctionType(const string& name) const {
        if (builtInVectorOperations->containsFunction(name)) {
            return FUNCTION;
        } else if (builtInAggregateFunctions->containsFunction(name)) {
            return AGGREGATE_FUNCTION;
        } else {
            throw CatalogException(name + " function does not exist.");
        }
    }

    /**
     * Node and Rel label functions.
     */

    void addNodeLabel(string labelName, const DataType& IDType,
        vector<PropertyNameDataType> colHeaderDefinitions,
        const vector<string>& unstructuredPropertyNames);

    void addRelLabel(string labelName, RelMultiplicity relMultiplicity,
        vector<PropertyNameDataType> colHeaderDefinitions, const vector<string>& srcNodeLabelNames,
        const vector<string>& dstNodeLabelNames);

    virtual inline string getNodeLabelName(label_t labelId) const {
        return nodeLabels[labelId].labelName;
    }
    virtual inline string getRelLabelName(label_t labelId) const {
        return relLabels[labelId].labelName;
    }

    inline RelLabel& getRel(label_t labelId) { return relLabels[labelId]; }

    inline uint64_t getNumNodeLabels() const { return nodeLabels.size(); }
    inline uint64_t getNumRelLabels() const { return relLabels.size(); }

    virtual inline bool containNodeLabel(const string& label) const {
        return end(nodeLabelNameToIdMap) != nodeLabelNameToIdMap.find(label);
    }
    virtual inline bool containRelLabel(const string& label) const {
        return end(relLabelNameToIdMap) != relLabelNameToIdMap.find(label);
    }

    virtual inline label_t getNodeLabelFromName(const string& label) const {
        return nodeLabelNameToIdMap.at(label);
    }
    virtual inline label_t getRelLabelFromName(const string& label) const {
        return relLabelNameToIdMap.at(label);
    }

    /**
     * Node and Rel property functions.
     */

    virtual bool containNodeProperty(label_t labelId, const string& propertyName) const;
    virtual bool containRelProperty(label_t relLabel, const string& propertyName) const;

    // getNodeProperty and getRelProperty should be called after checking if property exists
    // (containNodeProperty and containRelProperty).
    virtual const Property& getNodeProperty(label_t labelId, const string& propertyName) const;
    virtual const Property& getRelProperty(label_t labelId, const string& propertyName) const;

    vector<Property> getAllNodeProperties(label_t nodeLabel) const;
    inline const vector<Property>& getStructuredNodeProperties(label_t nodeLabel) const {
        return nodeLabels[nodeLabel].structuredProperties;
    }
    inline const vector<Property>& getUnstructuredNodeProperties(label_t nodeLabel) const {
        return nodeLabels[nodeLabel].unstructuredProperties;
    }
    inline const vector<Property>& getRelProperties(label_t relLabel) const {
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
        label_t relLabel, RelDirection direction) const;
    virtual const unordered_set<label_t>& getRelLabelsForNodeLabelDirection(
        label_t nodeLabel, RelDirection direction) const;

    virtual bool isSingleMultiplicityInDirection(label_t relLabel, RelDirection direction) const;

    virtual uint64_t getNumRelsForDirectionBoundLabel(
        label_t relLabel, RelDirection relDirection, label_t boundNodeLabel) const;

    void saveToFile(const string& directory);
    void readFromFile(const string& directory);

private:
    static void verifyColDefinitionsForNodeLabel(
        const string& primaryKeyName, const vector<PropertyNameDataType>& colHeaderDefinitions);
    static void verifyColDefinitionsForRelLabel(
        const vector<PropertyNameDataType>& colHeaderDefinitions);

private:
    shared_ptr<spdlog::logger> logger;
    unique_ptr<BuiltInVectorOperations> builtInVectorOperations;
    unique_ptr<BuiltInAggregateFunctions> builtInAggregateFunctions;
    vector<NodeLabel> nodeLabels;
    vector<RelLabel> relLabels;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    unordered_map<string, label_t> nodeLabelNameToIdMap;
    unordered_map<string, label_t> relLabelNameToIdMap;
};

} // namespace catalog
} // namespace graphflow
