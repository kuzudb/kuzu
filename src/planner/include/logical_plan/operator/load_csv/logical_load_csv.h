#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalLoadCSV : public LogicalOperator {

public:
    LogicalLoadCSV(string path, char tokenSeparator,
        vector<pair<string, DataType>> csvColumnVariableInfo,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, path{move(path)}, tokenSeparator{tokenSeparator},
          csvColumnVariableInfo{move(csvColumnVariableInfo)} {}

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
