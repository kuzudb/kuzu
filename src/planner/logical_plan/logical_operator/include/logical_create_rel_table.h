#pragma once

#include "base_logical_operator.h"
#include "logical_ddl.h"

namespace graphflow {
namespace planner {

class LogicalCreateRelTable : public LogicalDDL {

public:
    LogicalCreateRelTable(string labelName, vector<PropertyNameDataType> propertyNameDataTypes,
        RelMultiplicity relMultiplicity, SrcDstLabels srcDstLabels)
        : LogicalDDL{move(labelName), move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcDstLabels{move(srcDstLabels)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_CREATE_REL_TABLE;
    }

    inline RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline SrcDstLabels getSrcDstLabels() const { return srcDstLabels; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRelTable>(
            labelName, propertyNameDataTypes, relMultiplicity, srcDstLabels);
    }

private:
    RelMultiplicity relMultiplicity;
    SrcDstLabels srcDstLabels;
};

} // namespace planner
} // namespace graphflow
