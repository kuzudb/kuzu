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

void NodeLabel::addUnstructuredProperties(vector<string>& unstructuredPropertyNames) {
    for (auto& unstrPropertyName : unstructuredPropertyNames) {
        auto unstrPropertyId = unstructuredProperties.size();
        unstrPropertiesNameToIdMap[unstrPropertyName] = unstrPropertyId;
        Property property = Property::constructUnstructuredNodeProperty(
            unstrPropertyName, unstrPropertyId, labelId);
        unstructuredProperties.emplace_back(property);
    }
}

vector<Property> NodeLabel::getAllNodeProperties() const {
    auto allProperties = structuredProperties;
    allProperties.insert(
        allProperties.end(), unstructuredProperties.begin(), unstructuredProperties.end());
    return allProperties;
}

} // namespace catalog
} // namespace graphflow
