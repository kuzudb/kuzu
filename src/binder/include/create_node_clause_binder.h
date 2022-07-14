#pragma once

#include "src/binder/bound_create_node_clause/include/bound_create_node_clause.h"
#include "src/catalog/include/catalog.h"
#include "src/parser/create_node_clause/include/create_node_clause.h"

using namespace graphflow::parser;
using namespace graphflow::catalog;

namespace graphflow {
namespace binder {

class CreateNodeClauseBinder {
public:
    explicit CreateNodeClauseBinder(Catalog* catalog) : catalog{catalog} {}

    unique_ptr<BoundCreateNodeClause> bind(const CreateNodeClause& createNodeClause);

    /******* validations *********/
    inline void validateLabelName(string& schemaName) {
        if (catalog->containNodeLabel(schemaName)) {
            throw BinderException("Node " + schemaName + " already exists.");
        }
    }

    static string bindPrimaryKey(const CreateNodeClause& createNodeClause);

    static void validatePrimaryKey(string& primaryKey, vector<pair<string, string>>& properties);

    static vector<PropertyNameDataType> bindPropertyNameDataTypes(
        vector<pair<string, string>> propertyNameDataTypes);

private:
    Catalog* catalog;
};

} // namespace binder
} // namespace graphflow
