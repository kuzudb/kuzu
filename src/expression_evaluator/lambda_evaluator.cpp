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
}

void ListLambdaEvaluator::evaluate() {
    KU_ASSERT(children.size() == 1);
    children[0]->evaluate();
    auto listInputVector = children[0]->resultVector.get();
    auto& listInputSelVector = listInputVector->state->getSelVector();
    // NOTE: the following can be done with a memcpy. But I think soon we will need to change
    // to handle cases like
    // MATCH (a:person) RETURN LIST_TRANSFORM([1,2,3], x->x + a.ID)
    // So I'm leaving it in the naive form.
    for (auto i = 0u; i < listInputSelVector.getSelSize(); ++i) {
        auto pos = listInputSelVector[i];
        resultVector->setValue(pos, listInputVector->getValue<list_entry_t>(pos));
    }
    auto listSize = ListVector::getDataVectorSize(listInputVector);
    auto lambdaParamVector = lambdaParamEvaluator->resultVector.get();
    auto& lambdaParamSelVector = lambdaParamVector->state->getSelVectorUnsafe();
    lambdaParamSelVector.setSelSize(listSize);
    lambdaRootEvaluator->evaluate();
}

void ListLambdaEvaluator::resolveResultVector(const ResultSet&, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->getDataType().copy(), memoryManager);
    resultVector->state = children[0]->resultVector->state;
    ListVector::setDataVector(resultVector.get(), lambdaRootEvaluator->resultVector);
}

} // namespace evaluator
} // namespace kuzu
