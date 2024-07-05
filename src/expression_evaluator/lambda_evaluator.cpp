#include "expression_evaluator/lambda_evaluator.h"

#include "expression_evaluator/expression_evaluator_visitor.h"

using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::main;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

void ListLambdaEvaluator::init(const ResultSet& resultSet, ClientContext* clientContext) {
    for (auto& child : children) {
        child->init(resultSet, clientContext);
    }
    KU_ASSERT(children.size() == 1);
    auto listInputVector = children[0]->resultVector.get();
    auto collector = LambdaParamEvaluatorCollector();
    collector.visit(lambdaRootEvaluator.get());
    auto evaluators = collector.getEvaluators();
    auto lambdaVarState = std::make_shared<DataChunkState>();
    for (auto& evaluator : evaluators) {
        // For list_filter, list_transform:
        // The resultVector of lambdaEvaluator should be the list dataVector.
        // For list_reduce:
        // We should create two vectors for each lambda variable resultVector since we are going to
        // update the list elements during execution.
        evaluator->resultVector = evaluators.size() == 1 ?
                                      ListVector::getSharedDataVector(listInputVector) :
                                      std::make_shared<ValueVector>(
                                          ListType::getChildType(listInputVector->dataType).copy(),
                                          clientContext->getMemoryManager());
        evaluator->resultVector->state = lambdaVarState;
        lambdaParamEvaluators.push_back(evaluator->ptrCast<LambdaParamEvaluator>());
    }
    lambdaRootEvaluator->init(resultSet, clientContext);
    resolveResultVector(resultSet, clientContext->getMemoryManager());
    params.push_back(children[0]->resultVector);
    params.push_back(lambdaRootEvaluator->resultVector);
    bindData = ListLambdaBindData{lambdaParamEvaluators, lambdaRootEvaluator.get()};
}

void ListLambdaEvaluator::evaluate() {
    KU_ASSERT(children.size() == 1);
    children[0]->evaluate();
    execFunc(params, *resultVector, &bindData);
}

void ListLambdaEvaluator::resolveResultVector(const ResultSet&, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->getDataType().copy(), memoryManager);
    resultVector->state = children[0]->resultVector->state;
}

} // namespace evaluator
} // namespace kuzu
