#include "include/catalog_structs.h"

namespace graphflow {
namespace catalog {

void NodeLabel::addUnstructuredProperties(vector<string>& unstructuredPropertyNames) {
    for (auto& unstrPropertyName : unstructuredPropertyNames) {
        auto unstrPropertyId = unstructuredProperties.size();
        unstrPropertiesNameToIdMap[unstrPropertyName] = unstrPropertyId;
        Property property = Property::constructUnstructuredNodeProperty(
            unstrPropertyName, unstrPropertyId, labelId);
        unstructuredProperties.emplace_back(property);
    }
}

} // namespace catalog
} // namespace graphflow
