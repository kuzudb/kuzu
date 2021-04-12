#include "src/expression/include/physical/physical_expression.h"

namespace graphflow {
namespace expression {

bool PhysicalExpression::isResultFlat() {
    if (childrenExpr.size() == 0) {
        return result->isFlat();
    }
    for (auto& expr : childrenExpr) {
        if (!expr->isResultFlat()) {
            return false;
        }
    }
    return true;
}

shared_ptr<ValueVector> PhysicalExpression::createResultValueVector(DataType dataType) {
    auto valueVector = make_shared<ValueVector>(dataType, MAX_VECTOR_SIZE);
    auto dataChunk =
        make_shared<DataChunk>(true /* initializeSelectedValuesPos */, MAX_VECTOR_SIZE);
    valueVector->setDataChunkOwner(dataChunk);
    return valueVector;
}

} // namespace expression
} // namespace graphflow
