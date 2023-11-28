#include "expression_evaluator/node_rel_evaluator.h"

#include <memory>
#include <vector>

#include "common/vector/value_vector.h"
#include "expression_evaluator/expression_evaluator.h"
#include "function/struct/vector_struct_functions.h"
#include "processor/result/result_set.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace evaluator {

void NodeRelExpressionEvaluator::evaluate() {
    resultVector->resetAuxiliaryBuffer();
    for (auto& child : children) {
        child->evaluate();
    }
    StructPackFunctions::execFunc(parameters, *resultVector);
}

void NodeRelExpressionEvaluator::resolveResultVector(
    const processor::ResultSet& /*resultSet*/, storage::MemoryManager* memoryManager) {
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
