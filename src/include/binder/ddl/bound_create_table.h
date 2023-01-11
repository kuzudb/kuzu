#pragma once

#include "bound_ddl.h"
#include "catalog/catalog_structs.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

class BoundCreateTable : public BoundDDL {
public:
    explicit BoundCreateTable(StatementType statementType, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes)
        : BoundDDL{statementType, std::move(tableName)}, propertyNameDataTypes{
                                                             std::move(propertyNameDataTypes)} {}

    inline vector<PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

private:
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace binder
} // namespace kuzu
