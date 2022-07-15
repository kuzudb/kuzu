#include "include/create_node_clause_binder.h"

namespace graphflow {
namespace binder {

unique_ptr<BoundCreateNodeClause> CreateNodeClauseBinder::bind(
    const CreateNodeClause& createNodeClause) {
    auto labelName = createNodeClause.getLabelName();
    validateLabelName(labelName);
    auto propertyNameDataTypes =
        bindPropertyNameDataTypes(createNodeClause.getPropertyNameDataTypes());
    return make_unique<BoundCreateNodeClause>(
        move(labelName), bindPrimaryKey(createNodeClause), move(propertyNameDataTypes));
}

string CreateNodeClauseBinder::bindPrimaryKey(const CreateNodeClause& createNodeClause) {
    string primaryKey;
    auto propertyNameDataTypes = createNodeClause.getPropertyNameDataTypes();
    if (createNodeClause.hasPrimaryKey()) {
        primaryKey = createNodeClause.getPrimaryKey();
        validatePrimaryKey(primaryKey, propertyNameDataTypes);
    } else {
        // If the user doesn't give a primary key, then we use the first column as the default
        // primary key.
        primaryKey = propertyNameDataTypes[0].first;
    }
    return primaryKey;
}

void CreateNodeClauseBinder::validatePrimaryKey(
    string& primaryKey, vector<pair<string, string>>& properties) {
    for (auto& property : properties) {
        if (property.first == primaryKey) {
            // We only support INT and STRING column as primary key.
            if ((property.second != string("INT")) && (property.second != string("STRING"))) {
                throw BinderException("Invalid primary key type: " + property.second + ".");
            }
            return;
        }
    }
    throw BinderException("Invalid primary key: " + primaryKey + ".");
}

vector<PropertyNameDataType> CreateNodeClauseBinder::bindPropertyNameDataTypes(
    vector<pair<string, string>> propertyNameDataTypes) {
    vector<PropertyNameDataType> boundPropertyNameDataTypes;
    for (auto& propertyNameDataType : propertyNameDataTypes) {
        StringUtils::toUpper(propertyNameDataType.second);
        boundPropertyNameDataTypes.emplace_back(
            propertyNameDataType.first, Types::dataTypeFromString(propertyNameDataType.second));
    }
    return boundPropertyNameDataTypes;
}

} // namespace binder
} // namespace graphflow
