#pragma once

#include "base_logical_operator.h"
#include "logical_create_table.h"

namespace kuzu {
namespace planner {

class LogicalCreateRelTable : public LogicalCreateTable {
public:
    LogicalCreateRelTable(std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes,
        catalog::RelMultiplicity relMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, std::shared_ptr<binder::Expression> outputExpression)
        : LogicalCreateTable{LogicalOperatorType::CREATE_REL_TABLE, std::move(tableName),
              std::move(propertyNameDataTypes), std::move(outputExpression)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}

    inline catalog::RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline common::table_id_t getSrcTableID() const { return srcTableID; }

    inline common::table_id_t getDstTableID() const { return dstTableID; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRelTable>(tableName, propertyNameDataTypes, relMultiplicity,
            srcTableID, dstTableID, outputExpression);
    }

private:
    catalog::RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
};

} // namespace planner
} // namespace kuzu
