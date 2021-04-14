#include "src/processor/include/physical_plan/expression_mapper.h"

#include "src/common/include/expression_type.h"
#include "src/expression/include/logical/logical_expression.h"
#include "src/expression/include/physical/physical_binary_expression.h"
#include "src/expression/include/physical/physical_expression.h"
#include "src/expression/include/physical/physical_unary_expression.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

static unique_ptr<PhysicalExpression> mapLogicalLiteralExpressionToPhysical(
    const LogicalExpression& expression);

static unique_ptr<PhysicalExpression> mapLogicalPropertyExpressionToPhysical(
    const LogicalExpression& expression, PhysicalOperatorsInfo& physicalOperatorInfo);

unique_ptr<PhysicalExpression> ExpressionMapper::mapToPhysical(
    const LogicalExpression& expression, PhysicalOperatorsInfo& physicalOperatorInfo) {
    auto expressionType = expression.getExpressionType();
    if (isExpressionLeafLiteral(expressionType)) {
        return mapLogicalLiteralExpressionToPhysical(expression);
    } else if (isExpressionLeafVariable(expressionType)) {
        return mapLogicalPropertyExpressionToPhysical(expression, physicalOperatorInfo);
    } else if (isExpressionUnary(expressionType)) {
        auto child = mapToPhysical(expression.getChildExpr(0), physicalOperatorInfo);
        return make_unique<PhysicalUnaryExpression>(move(child), expression.getExpressionType());
    } else if (isExpressionBinary(expressionType)) {
        auto lExpr = mapToPhysical(expression.getChildExpr(0), physicalOperatorInfo);
        auto rExpr = mapToPhysical(expression.getChildExpr(1), physicalOperatorInfo);
        return make_unique<PhysicalBinaryExpression>(
            move(lExpr), move(rExpr), expression.getExpressionType());
    }
    // should never happen.
    throw std::invalid_argument("Unsupported expression type.");
}

unique_ptr<PhysicalExpression> ExpressionMapper::clone(const PhysicalExpression& expression) {
    if (expression.isLiteralLeafExpression()) {
        return make_unique<PhysicalExpression>(
            expression.literal, expression.expressionType, expression.dataType);
    } else if (expression.isPropertyLeafExpression()) {
        return make_unique<PhysicalExpression>(expression.valueVectorPos, expression.dataType);
    } else if (expression.getNumChildrenExpr() == 1) { // unary expression.
        return make_unique<PhysicalUnaryExpression>(
            clone(expression.getChildExpr(0)), expression.expressionType);
    } else { // binary expression.
        return make_unique<PhysicalBinaryExpression>(clone(expression.getChildExpr(0)),
            clone(expression.getChildExpr(1)), expression.expressionType);
    }
}

unique_ptr<PhysicalExpression> mapLogicalLiteralExpressionToPhysical(
    const LogicalExpression& expression) {
    return make_unique<PhysicalExpression>(
        expression.literalValue, expression.expressionType, expression.dataType);
}

unique_ptr<PhysicalExpression> mapLogicalPropertyExpressionToPhysical(
    const LogicalExpression& expression, PhysicalOperatorsInfo& physicalOperatorInfo) {
    const auto& variableName = expression.getVariableName();
    auto valueVectorPos = physicalOperatorInfo.getValueVectorPos(variableName);
    return make_unique<PhysicalExpression>(valueVectorPos, expression.dataType);
}

} // namespace processor
} // namespace graphflow
