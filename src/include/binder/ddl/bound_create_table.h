#pragma once

#include "bound_ddl.h"
#include "catalog/catalog_structs.h"

namespace kuzu {
namespace binder {

class BoundCreateTable : public BoundDDL {

public:
    explicit BoundCreateTable(common::StatementType statementType, std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes)
        : BoundDDL{statementType, std::move(tableName)}, propertyNameDataTypes{
                                                             std::move(propertyNameDataTypes)} {}

    inline std::vector<catalog::PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

private:
    std::vector<catalog::PropertyNameDataType> propertyNameDataTypes;
};

} // namespace binder
} // namespace kuzu
