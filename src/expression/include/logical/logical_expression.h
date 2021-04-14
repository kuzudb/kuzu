#pragma once

#include <unordered_set>

#include "src/common/include/expression_type.h"
#include "src/common/include/literal.h"
#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace expression {

// replace this with function enum once we have multiple default functions
const string COUNT_STAR = "COUNT_STAR";

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

    // creates a leaf literal expression.
    LogicalExpression(
        ExpressionType expressionType, DataType dataType, const Literal& literalValue);

    inline const string& getVariableName() const { return variableName; }

    inline const Literal& getLiteralValue() const { return literalValue; }

    inline DataType getDataType() const { return dataType; }

    inline ExpressionType getExpressionType() const { return expressionType; }

    inline const LogicalExpression& getChildExpr(uint64_t pos) const { return *childrenExpr[pos]; }

    inline const string& getRawExpression() const { return rawExpression; }

    unordered_set<string> getIncludedVariables() const;

    unordered_set<string> getIncludedProperties() const;

protected:
    LogicalExpression(ExpressionType expressionType, DataType dataType)
        : expressionType{expressionType}, dataType{dataType} {}

public:
    // variable name for leaf variable expressions.
    string variableName;
    // value used by leaf literal expressions.
    Literal literalValue;
    vector<shared_ptr<LogicalExpression>> childrenExpr;
    ExpressionType expressionType;
    DataType dataType;
    string alias;
    string rawExpression;
};

} // namespace expression
} // namespace graphflow
