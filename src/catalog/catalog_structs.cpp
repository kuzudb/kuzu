#include "include/catalog_structs.h"

#include "src/common/include/exception.h"

namespace kuzu {
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

string getRelMultiplicityAsString(RelMultiplicity relMultiplicity) {
    switch (relMultiplicity) {
    case MANY_MANY: {
        return "MANY_MANY";
    }
    case MANY_ONE: {
        return "MANY_ONE";
    }
    case ONE_ONE: {
        return "ONE_ONE";
    }
    case ONE_MANY: {
        return "ONE_MANY";
    }
    default:
        assert(false);
    }
}

void NodeTableSchema::addUnstructuredProperties(vector<string>& unstructuredPropertyNames) {
    // TODO(Semih): Uncomment when enabling ad-hoc properties
    assert(unstructuredProperties.empty());
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

unordered_set<table_id_t> RelTableSchema::getAllNodeTableIDs() const {
    unordered_set<table_id_t> allNodeTableIDs;
    allNodeTableIDs.insert(srcDstTableIDs.srcTableIDs.begin(), srcDstTableIDs.srcTableIDs.end());
    allNodeTableIDs.insert(srcDstTableIDs.dstTableIDs.begin(), srcDstTableIDs.dstTableIDs.end());
    return allNodeTableIDs;
}

} // namespace catalog
} // namespace kuzu
