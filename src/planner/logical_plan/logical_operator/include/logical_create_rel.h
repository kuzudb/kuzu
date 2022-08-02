#pragma once

#include "base_logical_operator.h"
#include "logical_ddl.h"

namespace graphflow {
namespace planner {

class LogicalCreateRel : public LogicalDDL {

public:
    LogicalCreateRel(string labelName, vector<PropertyNameDataType> propertyNameDataTypes,
        RelMultiplicity relMultiplicity, vector<pair<label_t, label_t>> relConnections)
        : LogicalDDL{move(labelName), move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, relConnections{move(relConnections)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_CREATE_REL;
    }

    RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    vector<pair<label_t, label_t>> getRelConnections() const { return relConnections; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRel>(
            labelName, propertyNameDataTypes, relMultiplicity, relConnections);
    }

private:
    RelMultiplicity relMultiplicity;
    vector<pair<label_t, label_t>> relConnections;
};

} // namespace planner
} // namespace graphflow
