#pragma once

#include "base_logical_operator.h"
#include "logical_create_table.h"

namespace kuzu {
namespace planner {

class LogicalCreateRelTable : public LogicalCreateTable {
public:
    LogicalCreateRelTable(string tableName, vector<PropertyNameDataType> propertyNameDataTypes,
        RelMultiplicity relMultiplicity, vector<pair<table_id_t, table_id_t>> srcDstTableIDs,
        shared_ptr<Expression> outputExpression)
        : LogicalCreateTable{LogicalOperatorType::CREATE_REL_TABLE, std::move(tableName),
              std::move(propertyNameDataTypes), std::move(outputExpression)},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{std::move(srcDstTableIDs)} {}

    inline RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline vector<pair<table_id_t, table_id_t>> getSrcDstTableIDs() const { return srcDstTableIDs; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRelTable>(
            tableName, propertyNameDataTypes, relMultiplicity, srcDstTableIDs, outputExpression);
    }

private:
    RelMultiplicity relMultiplicity;
    vector<pair<table_id_t, table_id_t>> srcDstTableIDs;
};

} // namespace planner
} // namespace kuzu
