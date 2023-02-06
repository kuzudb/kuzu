#pragma once

#include "base_logical_operator.h"
#include "logical_create_table.h"

namespace kuzu {
namespace planner {

class LogicalCreateRelTable : public LogicalCreateTable {
public:
    LogicalCreateRelTable(std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes,
        catalog::RelMultiplicity relMultiplicity,
        std::vector<std::pair<common::table_id_t, common::table_id_t>> srcDstTableIDs,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalCreateTable{LogicalOperatorType::CREATE_REL_TABLE, std::move(tableName),
              std::move(propertyNameDataTypes), std::move(outputExpression)},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{std::move(srcDstTableIDs)} {}

    inline catalog::RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline std::vector<std::pair<common::table_id_t, common::table_id_t>>
    getSrcDstTableIDs() const {
        return srcDstTableIDs;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRelTable>(
            tableName, propertyNameDataTypes, relMultiplicity, srcDstTableIDs, outputExpression);
    }

private:
    catalog::RelMultiplicity relMultiplicity;
    std::vector<std::pair<common::table_id_t, common::table_id_t>> srcDstTableIDs;
};

} // namespace planner
} // namespace kuzu
