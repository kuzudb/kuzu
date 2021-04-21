#include "src/expression/include/physical/physical_unary_expression.h"

namespace graphflow {
namespace expression {

PhysicalUnaryExpression::PhysicalUnaryExpression(
    unique_ptr<PhysicalExpression> child, ExpressionType expressionType, DataType dataType) {
    childrenExpr.push_back(move(child));
    this->expressionType = expressionType;
    this->dataType = dataType;
    operation = ValueVector::getUnaryOperation(expressionType);
    result = createResultValueVector();
}

void PhysicalUnaryExpression::evaluate() {
    childrenExpr[0]->evaluate();
    operation(*childrenExpr[0]->result, *result);
}

shared_ptr<ValueVector> PhysicalUnaryExpression::createResultValueVector() {
    auto valueVector = make_shared<ValueVector>(dataType, MAX_VECTOR_SIZE);
    if (expressionType == CAST_TO_UNKNOWN) {
        auto unstructuredValues = (Value*)valueVector->values;
        for (auto i = 0u; i < MAX_VECTOR_SIZE; i++) {
            unstructuredValues[i].dataType = childrenExpr[0]->dataType;
        }
    }
    valueVector->state = childrenExpr[0]->result->state;
    return valueVector;
}

} // namespace expression
} // namespace graphflow
