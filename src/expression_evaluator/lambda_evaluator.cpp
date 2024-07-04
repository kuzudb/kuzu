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
    lambdaParamEvaluator = evaluators[0]->ptrCast<LambdaParamEvaluator>();
    lambdaParamEvaluator->resultVector = ListVector::getSharedDataVector(listInputVector);
    lambdaParamEvaluator->resultVector->state = std::make_shared<DataChunkState>();
    lambdaRootEvaluator->init(resultSet, clientContext);
    resolveResultVector(resultSet, clientContext->getMemoryManager());
    params.push_back(children[0]->resultVector);
    params.push_back(lambdaRootEvaluator->resultVector);
}

void ListLambdaEvaluator::evaluate() {
    KU_ASSERT(children.size() == 1);
    children[0]->evaluate();
    auto listSize = ListVector::getDataVectorSize(children[0]->resultVector.get());
    auto lambdaParamVector = lambdaParamEvaluator->resultVector.get();
    lambdaParamVector->state->getSelVectorUnsafe().setSelSize(listSize);
    lambdaRootEvaluator->evaluate();
    execFunc(params, *resultVector, nullptr /* dataPtr */);
}

void ListLambdaEvaluator::resolveResultVector(const ResultSet&, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->getDataType().copy(), memoryManager);
    resultVector->state = children[0]->resultVector->state;
}

} // namespace evaluator
} // namespace kuzu
