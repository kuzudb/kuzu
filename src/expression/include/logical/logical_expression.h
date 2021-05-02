#pragma once

#include <cassert>
#include <unordered_set>

#include "src/common/include/expression_type.h"
#include "src/common/include/types.h"
#include "src/common/include/value.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace expression {

// replace this with function enum once we have more functions
const string FUNCTION_COUNT_STAR = "COUNT_STAR";
const string FUNCTION_ID = "ID";

class LogicalExpression {

public:
    // creates a non-leaf logical binary expression.
    LogicalExpression(ExpressionType expressionType, DataType dataType,
        shared_ptr<LogicalExpression> left, shared_ptr<LogicalExpression> right);

    // creates a non-leaf logical unary expression.
    LogicalExpression(
        ExpressionType expressionType, DataType dataType, shared_ptr<LogicalExpression> child);

    // creates a leaf variable expression.
    LogicalExpression(ExpressionType expressionType, DataType dataType, const string& variableName);

    virtual void cast(DataType dataTypeToCast) {
        throw invalid_argument("LogicalExpression does not support cast(DataType).");
    }

    inline const LogicalExpression& getChildExpr(uint64_t pos) const { return *childrenExpr[pos]; }

    inline const string getAliasElseRawExpression() const {
        return alias.empty() ? rawExpression : alias;
    }

    virtual unordered_set<string> getIncludedVariables() const;

    unordered_set<string> getIncludedProperties() const;

protected:
    LogicalExpression(ExpressionType expressionType, DataType dataType)
        : expressionType{expressionType}, dataType{dataType} {}

public:
    // variable name for leaf variable expressions.
    string variableName;
    vector<shared_ptr<LogicalExpression>> childrenExpr;
    ExpressionType expressionType;
    DataType dataType;
    string alias;
    string rawExpression;
};

} // namespace expression
} // namespace graphflow
