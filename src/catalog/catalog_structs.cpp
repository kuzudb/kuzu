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

void NodeTableSchema::addAdhocProperties(vector<PropertyNameDataType>& adhocPropertyNameDataTypes) {
    for (auto& adhocPropertyNameDataType : adhocPropertyNameDataTypes) {
        auto adhocPropertyId = adhocProperties.size();
        adhocPropertiesNameToIdMap[adhocPropertyNameDataType.name] = adhocPropertyId;
        adhocProperties.emplace_back(Property(std::move(adhocPropertyNameDataType.name),
            DataType(UNSTRUCTURED), adhocPropertyId, tableID));
    }
}

vector<Property> NodeTableSchema::getAllNodeProperties() const {
    auto allProperties = predefinedProperties;
    allProperties.insert(allProperties.end(), adhocProperties.begin(), adhocProperties.end());
    return allProperties;
}

unordered_set<table_id_t> RelTableSchema::getAllNodeTableIDs() const {
    unordered_set<table_id_t> allNodeTableIDs;
    allNodeTableIDs.insert(srcDstTableIDs.srcTableIDs.begin(), srcDstTableIDs.srcTableIDs.end());
    allNodeTableIDs.insert(srcDstTableIDs.dstTableIDs.begin(), srcDstTableIDs.dstTableIDs.end());
    return allNodeTableIDs;
}

} // namespace catalog
} // namespace graphflow
