#pragma once

#include "src/binder/bound_statement/include/bound_statement.h"
#include "src/catalog/include/catalog_structs.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace binder {

class BoundDDL : public BoundStatement {
public:
    explicit BoundDDL(StatementType statementType, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes)
        : BoundStatement{statementType}, tableName{move(tableName)}, propertyNameDataTypes{move(
                                                                         propertyNameDataTypes)} {}

    inline string getTableName() const { return tableName; }
    inline vector<PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

private:
    string tableName;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace binder
} // namespace graphflow
