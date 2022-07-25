#pragma once

#include "src/binder/bound_statement/include/bound_statement.h"
#include "src/catalog/include/catalog_structs.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace binder {

class BoundCreateNodeClause : public BoundStatement {
public:
    explicit BoundCreateNodeClause(
        string labelName, string primaryKey, vector<PropertyNameDataType> propertyNameDataTypes)
        : BoundStatement{StatementType::CreateNodeClause}, labelName{move(labelName)},
          primaryKey{move(primaryKey)}, propertyNameDataTypes{move(propertyNameDataTypes)} {}

    inline string getLabelName() const { return labelName; }
    inline string getPrimaryKey() const { return primaryKey; }
    inline vector<PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

private:
    string labelName;
    string primaryKey;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace binder
} // namespace graphflow
