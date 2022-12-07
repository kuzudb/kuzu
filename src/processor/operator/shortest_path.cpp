#include "processor/operator/shortest_path.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> ShortestPath::init(kuzu::processor::ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    srcNodeExprEvaluator->init(*resultSet, context->memoryManager);
    destNodeExprEvaluator->init(*resultSet, context->memoryManager);
    return resultSet;
}

bool ShortestPath::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    srcNodeExprEvaluator->evaluate();
    destNodeExprEvaluator->evaluate();
    // src node vector -> srcNodeExprEvaluator->resultVector;
    // dst node vector -> destNodeExprEvaluator->resultVector;
    return true;
}

} // namespace processor
} // namespace kuzu