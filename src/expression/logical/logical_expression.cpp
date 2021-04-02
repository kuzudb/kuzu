#include "src/expression/include/logical/logical_expression.h"

namespace graphflow {
namespace expression {

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    unique_ptr<LogicalExpression> left, unique_ptr<LogicalExpression> right, string rawExpression)
    : LogicalExpression(expressionType, dataType) {
    childrenExpr.push_back(move(left));
    childrenExpr.push_back(move(right));
    this->rawExpression = move(rawExpression);
}

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    unique_ptr<LogicalExpression> child, string rawExpression)
    : LogicalExpression(expressionType, dataType) {
    childrenExpr.push_back(move(child));
    this->rawExpression = move(rawExpression);
}

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    const string& variableName, string rawExpression)
    : LogicalExpression(expressionType, dataType) {
    this->variableName = variableName;
    this->rawExpression = move(rawExpression);
}

LogicalExpression::LogicalExpression(ExpressionType expressionType, DataType dataType,
    const Literal& literalValue, string rawExpression)
    : LogicalExpression(expressionType, dataType) {
    this->literalValue = literalValue;
    this->rawExpression = move(rawExpression);
}

} // namespace expression
} // namespace graphflow
