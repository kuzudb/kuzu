#include "src/expression/include/logical/logical_expression.h"

namespace graphflow {
namespace expression {

LogicalExpression::LogicalExpression(EXPRESSION_TYPE expressionType,
    unique_ptr<LogicalExpression> left, unique_ptr<LogicalExpression> right) {
    childrenExpr.push_back(move(left));
    childrenExpr.push_back(move(right));
    this->expressionType = expressionType;
}

LogicalExpression::LogicalExpression(
    EXPRESSION_TYPE expressionType, unique_ptr<LogicalExpression> child) {
    childrenExpr.push_back(move(child));
    this->expressionType = expressionType;
}

LogicalExpression::LogicalExpression(
    EXPRESSION_TYPE expressionType, DataType dataType, const string& variableName)
    : LogicalExpression(expressionType, dataType) {
    this->variableName = variableName;
}

LogicalExpression::LogicalExpression(
    EXPRESSION_TYPE expressionType, DataType dataType, const Literal& literalValue)
    : LogicalExpression(expressionType, dataType) {
    this->literalValue = literalValue;
}

} // namespace expression
} // namespace graphflow
