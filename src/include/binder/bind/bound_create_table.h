#pragma once

#include "binder/bound_statement.h"
#include "catalog/catalog_structs.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

class BoundCreateTable : public BoundStatement {
public:
    explicit BoundCreateTable(StatementType statementType, string tableName,
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
} // namespace kuzu
