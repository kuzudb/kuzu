#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalLoadCSV : public LogicalOperator {

public:
    LogicalLoadCSV(
        string path, char tokenSeparator, vector<shared_ptr<Expression>> csvColumnVariables)
        : path{move(path)}, tokenSeparator{tokenSeparator}, csvColumnVariables{
                                                                move(csvColumnVariables)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_LOAD_CSV;
    }

    string getOperatorInformation() const override { return path; }

public:
    string path;
    char tokenSeparator;
    vector<shared_ptr<Expression>> csvColumnVariables;
};

} // namespace planner
} // namespace graphflow
