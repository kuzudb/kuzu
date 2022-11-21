#pragma once

#include "base_logical_operator.h"
#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateRelTable : public LogicalDDL {

public:
    LogicalCreateRelTable(string tableName, vector<PropertyNameDataType> propertyNameDataTypes,
        RelMultiplicity relMultiplicity, vector<pair<table_id_t, table_id_t>> srcDstTableIDs)
        : LogicalDDL{move(tableName), move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcDstTableIDs{move(srcDstTableIDs)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_CREATE_REL_TABLE;
    }

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
