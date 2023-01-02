#pragma once

#include "base_logical_operator.h"
#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateRelTable : public LogicalDDL {
public:
    LogicalCreateRelTable(string tableName, vector<PropertyNameDataType> propertyNameDataTypes,
        RelMultiplicity relMultiplicity, vector<pair<table_id_t, table_id_t>> srcDstTableIDs)
        : LogicalDDL{LogicalOperatorType::CREATE_REL_TABLE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{std::move(srcDstTableIDs)} {}

    inline RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline vector<pair<table_id_t, table_id_t>> getSrcDstTableIDs() const { return srcDstTableIDs; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRelTable>(
            tableName, propertyNameDataTypes, relMultiplicity, srcDstTableIDs);
    }

private:
    RelMultiplicity relMultiplicity;
    vector<pair<table_id_t, table_id_t>> srcDstTableIDs;
};

} // namespace planner
} // namespace kuzu
