#include "expression_evaluator/node_rel_evaluator.h"

#include "function/struct/vector_struct_functions.h"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace evaluator {

void NodeRelExpressionEvaluator::evaluate(ClientContext* clientContext) {
    for (auto& child : children) {
        child->evaluate(clientContext);
    }
    StructPackFunctions::execFunc(parameters, *resultVector);
    auto structType = nodeOrRel->getDataType();
    auto internalIDIdx = StructType::getFieldIdx(structType, InternalKeyword::ID);
    auto internalIDVector = StructVector::getFieldVector(resultVector.get(), internalIDIdx);
    for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); ++i) {
        auto pos = resultVector->state->getSelVector()[i];
        resultVector->setNull(pos, internalIDVector->isNull(pos));
    }
}

void NodeRelExpressionEvaluator::resolveResultVector(const processor::ResultSet& /*resultSet*/,
    storage::MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(nodeOrRel->getDataType(), memoryManager);
    std::vector<ExpressionEvaluator*> inputEvaluators;
    inputEvaluators.reserve(children.size());
    for (auto& child : children) {
        parameters.push_back(child->resultVector);
        inputEvaluators.push_back(child.get());
    }
    resolveResultStateFromChildren(inputEvaluators);
    StructPackFunctions::compileFunc(nullptr, parameters, resultVector);
}

} // namespace evaluator
} // namespace kuzu
