#include "include/catalog_structs.h"

#include "src/common/include/exception.h"

namespace graphflow {
namespace catalog {

RelMultiplicity getRelMultiplicityFromString(const string& relMultiplicityString) {
    if ("ONE_ONE" == relMultiplicityString) {
        return ONE_ONE;
    } else if ("MANY_ONE" == relMultiplicityString) {
        return MANY_ONE;
    } else if ("ONE_MANY" == relMultiplicityString) {
        return ONE_MANY;
    } else if ("MANY_MANY" == relMultiplicityString) {
        return MANY_MANY;
    }
    throw CatalogException("Invalid relMultiplicity string \"" + relMultiplicityString + "\"");
}

void NodeTableSchema::addUnstructuredProperties(vector<string>& unstructuredPropertyNames) {
    for (auto& unstrPropertyName : unstructuredPropertyNames) {
        auto unstrPropertyId = unstructuredProperties.size();
        unstrPropertiesNameToIdMap[unstrPropertyName] = unstrPropertyId;
        Property property = Property::constructUnstructuredNodeProperty(
            unstrPropertyName, unstrPropertyId, tableID);
        unstructuredProperties.emplace_back(property);
    }
}

vector<Property> NodeTableSchema::getAllNodeProperties() const {
    auto allProperties = structuredProperties;
    allProperties.insert(
        allProperties.end(), unstructuredProperties.begin(), unstructuredProperties.end());
    return allProperties;
}

RelTableSchema::RelTableSchema(string tableName, table_id_t tableID,
    RelMultiplicity relMultiplicity, vector<Property> properties, SrcDstTableIDs srcDstTableIDs)
    : TableSchema{move(tableName), tableID, false /* isNodeTable */},
      relMultiplicity{relMultiplicity}, numGeneratedProperties{1}, properties{move(properties)},
      srcDstTableIDs{move(srcDstTableIDs)}, numRels{0} {
    numRelsPerDirectionBoundTableID.resize(2);
    for (auto srcTableID : this->srcDstTableIDs.srcTableIDs) {
        numRelsPerDirectionBoundTableID[RelDirection::FWD].emplace(srcTableID, 0);
    }
    for (auto dstTableID : this->srcDstTableIDs.dstTableIDs) {
        numRelsPerDirectionBoundTableID[RelDirection::BWD].emplace(dstTableID, 0);
    }
}

unordered_set<table_id_t> RelTableSchema::getAllNodeTableIDs() const {
    unordered_set<table_id_t> allNodeTableIDs;
    allNodeTableIDs.insert(srcDstTableIDs.srcTableIDs.begin(), srcDstTableIDs.srcTableIDs.end());
    allNodeTableIDs.insert(srcDstTableIDs.dstTableIDs.begin(), srcDstTableIDs.dstTableIDs.end());
    return allNodeTableIDs;
}

} // namespace catalog
} // namespace graphflow
