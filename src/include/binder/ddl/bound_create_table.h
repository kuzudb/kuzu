#pragma once

#include "bound_ddl.h"
#include "catalog/table_schema.h"

namespace kuzu {
namespace binder {

class BoundCreateTable : public BoundDDL {

public:
    explicit BoundCreateTable(common::StatementType statementType, std::string tableName,
        std::vector<std::unique_ptr<catalog::Property>> properties)
        : BoundDDL{statementType, std::move(tableName)}, properties{std::move(properties)} {}

    inline std::vector<std::unique_ptr<catalog::Property>> getProperties() const {
        return catalog::Property::copyProperties(properties);
    }

private:
    std::vector<std::unique_ptr<catalog::Property>> properties;
};

} // namespace binder
} // namespace kuzu
