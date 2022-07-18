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

class CatalogContent {
public:
    CatalogContent();
    explicit CatalogContent(const string& directory);

    CatalogContent(const CatalogContent& other);

    virtual ~CatalogContent() = default;

    /**
     * Node and Rel label functions.
     */
    unique_ptr<NodeLabel> createNodeLabel(string labelName, string primaryKey,
        vector<PropertyNameDataType> structuredPropertyDefinitions);

    inline void addNodeLabel(unique_ptr<NodeLabel> nodeLabel) {
        nodeLabelNameToIdMap[nodeLabel->labelName] = nodeLabel->labelId;
        nodeLabels.push_back(move(nodeLabel));
    }

    unique_ptr<RelLabel> createRelLabel(string labelName, RelMultiplicity relMultiplicity,
        vector<PropertyNameDataType>& structuredPropertyDefinitions,
        const vector<string>& srcNodeLabelNames, const vector<string>& dstNodeLabelNames);

    inline void addRelLabel(unique_ptr<RelLabel> relLabel) {
        relLabelNameToIdMap[relLabel->labelName] = relLabel->labelId;
        relLabels.push_back(move(relLabel));
    }

    virtual inline string getNodeLabelName(label_t labelId) const {
        return nodeLabels[labelId]->labelName;
    }
    virtual inline string getRelLabelName(label_t labelId) const {
        return relLabels[labelId]->labelName;
    }

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

    const Property& getNodePrimaryKeyProperty(label_t nodeLabel) const;

    vector<Property> getAllNodeProperties(label_t nodeLabel) const;
    inline const vector<Property>& getStructuredNodeProperties(label_t nodeLabel) const {
        return nodeLabels[nodeLabel]->structuredProperties;
    }
    inline const vector<Property>& getUnstructuredNodeProperties(label_t nodeLabel) const {
        return nodeLabels[nodeLabel]->unstructuredProperties;
    }
    inline const vector<Property>& getRelProperties(label_t relLabel) const {
        return relLabels[relLabel]->properties;
    }
    inline const unordered_map<string, uint64_t>& getUnstrPropertiesNameToIdMap(
        label_t nodeLabel) const {
        return nodeLabels[nodeLabel]->unstrPropertiesNameToIdMap;
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

    void saveToFile(const string& directory, bool isForWALRecord);
    void readFromFile(const string& directory);

private:
    static void verifyColDefinitionsForNodeLabel(
        const string& primaryKeyName, const vector<PropertyNameDataType>& colHeaderDefinitions);
    static void verifyStructurePropertyDefinitionsForRelLabel(
        const vector<PropertyNameDataType>& colHeaderDefinitions);

private:
    shared_ptr<spdlog::logger> logger;
    vector<unique_ptr<NodeLabel>> nodeLabels;
    vector<unique_ptr<RelLabel>> relLabels;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    unordered_map<string, label_t> nodeLabelNameToIdMap;
    unordered_map<string, label_t> relLabelNameToIdMap;
};

class Catalog {
public:
    Catalog();
    explicit Catalog(const string& directory);

    virtual ~Catalog() = default;

    inline CatalogContent* getReadOnlyVersion() const { return catalogContentForReadOnlyTrx.get(); }

    inline BuiltInVectorOperations* getBuiltInScalarFunctions() const {
        return builtInVectorOperations.get();
    }
    inline BuiltInAggregateFunctions* getBuiltInAggregateFunction() const {
        return builtInAggregateFunctions.get();
    }

    virtual inline unique_ptr<NodeLabel> createNodeLabel(string labelName, string primaryKey,
        vector<PropertyNameDataType> structuredPropertyDefinitions) {
        initCatalogContentForWriteTrxIfNecessary();
        return catalogContentForWriteTrx->createNodeLabel(
            move(labelName), move(primaryKey), move(structuredPropertyDefinitions));
    }

    virtual inline void addNodeLabel(unique_ptr<NodeLabel> nodeLabel) {
        initCatalogContentForWriteTrxIfNecessary();
        catalogContentForWriteTrx->addNodeLabel(move(nodeLabel));
    }

    virtual inline unique_ptr<RelLabel> createRelLabel(string labelName,
        RelMultiplicity relMultiplicity,
        vector<PropertyNameDataType>& structuredPropertyDefinitions,
        const vector<string>& srcNodeLabelNames, const vector<string>& dstNodeLabelNames) {
        initCatalogContentForWriteTrxIfNecessary();
        return catalogContentForWriteTrx->createRelLabel(move(labelName), relMultiplicity,
            structuredPropertyDefinitions, srcNodeLabelNames, dstNodeLabelNames);
    }

    virtual inline void addRelLabel(unique_ptr<RelLabel> relLabel) {
        initCatalogContentForWriteTrxIfNecessary();
        catalogContentForWriteTrx->addRelLabel(move(relLabel));
    }

    inline bool hasUpdates() { return catalogContentForWriteTrx != nullptr; }

    void checkpointInMemoryIfNecessary();

    inline void initCatalogContentForWriteTrxIfNecessary() {
        if (!catalogContentForWriteTrx) {
            catalogContentForWriteTrx = make_unique<CatalogContent>(*catalogContentForReadOnlyTrx);
        }
    }

    inline void writeCatalogForWALRecord(string directory) {
        catalogContentForWriteTrx->saveToFile(move(directory), true /* isForWALRecord */);
    }

    ExpressionType getFunctionType(const string& name) const;

protected:
    unique_ptr<BuiltInVectorOperations> builtInVectorOperations;
    unique_ptr<BuiltInAggregateFunctions> builtInAggregateFunctions;
    unique_ptr<CatalogContent> catalogContentForReadOnlyTrx;
    unique_ptr<CatalogContent> catalogContentForWriteTrx;
};

// This class is only used by loader to initialize node/rel labels. Since the loader has no
// knowledge of transaction, the catalogBuilder always read/update the catalogContentForReadOnly
// transactions.
class CatalogBuilder : public Catalog {
public:
    CatalogBuilder() : Catalog() {}

    inline unique_ptr<NodeLabel> createNodeLabel(string labelName, string primaryKey,
        vector<PropertyNameDataType> structuredPropertyDefinitions) override {
        return catalogContentForReadOnlyTrx->createNodeLabel(
            move(labelName), move(primaryKey), move(structuredPropertyDefinitions));
    }

    inline void addNodeLabel(unique_ptr<NodeLabel> nodeLabel) override {
        catalogContentForReadOnlyTrx->addNodeLabel(move(nodeLabel));
    }

    inline unique_ptr<RelLabel> createRelLabel(string labelName, RelMultiplicity relMultiplicity,
        vector<PropertyNameDataType>& structuredPropertyDefinitions,
        const vector<string>& srcNodeLabelNames, const vector<string>& dstNodeLabelNames) override {
        return catalogContentForReadOnlyTrx->createRelLabel(move(labelName), relMultiplicity,
            structuredPropertyDefinitions, srcNodeLabelNames, dstNodeLabelNames);
    }

    inline void addRelLabel(unique_ptr<RelLabel> relLabel) override {
        catalogContentForReadOnlyTrx->addRelLabel(move(relLabel));
    }
};

} // namespace catalog
} // namespace graphflow
