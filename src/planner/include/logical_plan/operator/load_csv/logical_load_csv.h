#pragma once

#include "src/expression/include/logical/logical_expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::expression;

namespace graphflow {
namespace planner {

class LogicalLoadCSV : public LogicalOperator {

public:
    LogicalLoadCSV(string path, vector<pair<string, DataType>> csvColumnVariableInfo,
        shared_ptr<LogicalOperator> prevOperator, char tokenSeparator = ',')
        : LogicalOperator{prevOperator}, path{path}, tokenSeparator{tokenSeparator},
          csvColumnVariableInfo{csvColumnVariableInfo} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_LOAD_CSV;
    }

    string getOperatorInformation() const override { return path; }

public:
    string path;
    char tokenSeparator;
    vector<pair<string, DataType>> csvColumnVariableInfo;
};

} // namespace planner
} // namespace graphflow
