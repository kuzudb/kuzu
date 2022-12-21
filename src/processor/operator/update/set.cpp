#include "processor/operator/update/set.h"

namespace kuzu {
namespace processor {

void BaseSetNodeProperty::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& pos : nodeIDPositions) {
        auto nodeIDVector = resultSet->getValueVector(pos);
        nodeIDVectors.push_back(std::move(nodeIDVector));
    }
    for (auto& expressionEvaluator : expressionEvaluators) {
        expressionEvaluator->init(*resultSet, context->memoryManager);
    }
}

bool SetNodeStructuredProperty::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    for (auto i = 0u; i < nodeIDVectors.size(); ++i) {
        expressionEvaluators[i]->evaluate();
        columns[i]->writeValues(nodeIDVectors[i], expressionEvaluators[i]->resultVector);
    }
    return true;
}

unique_ptr<PhysicalOperator> SetNodeStructuredProperty::clone() {
    vector<unique_ptr<BaseExpressionEvaluator>> clonedExpressionEvaluators;
    for (auto& expressionEvaluator : expressionEvaluators) {
        clonedExpressionEvaluators.push_back(expressionEvaluator->clone());
    }
    return make_unique<SetNodeStructuredProperty>(nodeIDPositions,
        std::move(clonedExpressionEvaluators), columns, children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
