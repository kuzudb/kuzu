#pragma once

#include "bound_create_table.h"

namespace kuzu {
namespace binder {

class BoundCreateRelClause : public BoundCreateTable {
public:
    explicit BoundCreateRelClause(std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes,
        catalog::RelMultiplicity relMultiplicity,
        std::vector<std::pair<common::table_id_t, common::table_id_t>> srcDstTableIDs)
        : BoundCreateTable{common::StatementType::CREATE_REL_CLAUSE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{std::move(srcDstTableIDs)} {}

    catalog::RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    std::vector<std::pair<common::table_id_t, common::table_id_t>> getSrcDstTableIDs() const {
        return srcDstTableIDs;
    }

private:
    catalog::RelMultiplicity relMultiplicity;
    std::vector<std::pair<common::table_id_t, common::table_id_t>> srcDstTableIDs;
};

} // namespace binder
} // namespace kuzu
