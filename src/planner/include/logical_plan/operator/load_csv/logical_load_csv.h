#pragma once

#include "src/binder/include/expression/variable_expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalLoadCSV : public LogicalOperator {

public:
    LogicalLoadCSV(
        string path, char tokenSeparator, vector<shared_ptr<VariableExpression>> csvColumnVariables)
        : path{move(path)}, tokenSeparator{tokenSeparator}, csvColumnVariables{
                                                                move(csvColumnVariables)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_LOAD_CSV;
    }

    string getExpressionsForPrinting() const override { return path; }

public:
    string path;
    char tokenSeparator;
    vector<shared_ptr<VariableExpression>> csvColumnVariables;
};

} // namespace planner
} // namespace graphflow
