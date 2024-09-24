#include "expression_evaluator/lambda_evaluator.h"

#include "binder/expression/lambda_expression.h"
#include "binder/expression/scalar_function_expression.h"
#include "common/exception/runtime.h"
#include "expression_evaluator/expression_evaluator_visitor.h"
#include "function/list/vector_list_functions.h"
#include "parser/expression/parsed_lambda_expression.h"

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
    // Find all param in lambda, e.g. find x in x->x+1
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
        evaluator->resultVector = listLambdaType != ListLambdaType::LIST_REDUCE ?
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
    auto paramIndices = getParamIndices();
    bindData = ListLambdaBindData{lambdaParamEvaluators, paramIndices, lambdaRootEvaluator.get()};
}

void ListLambdaEvaluator::evaluate() {
    KU_ASSERT(children.size() == 1);
    children[0]->evaluate();
    execFunc(params, *resultVector, &bindData);
}

void ListLambdaEvaluator::resolveResultVector(const ResultSet&, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(expression->getDataType().copy(), memoryManager);
    resultVector->state = children[0]->resultVector->state;
    // Performance optimization to avoid copy data vector for list_transform
    if (expression->cast<binder::ScalarFunctionExpression>().getFunction().name ==
        function::ListTransformFunction::name) {
        ListVector::setDataVector(resultVector.get(), lambdaRootEvaluator->resultVector);
    }
    isResultFlat_ = children[0]->isResultFlat();
}
std::vector<common::idx_t> ListLambdaEvaluator::getParamIndices() {
    const auto& paramNames = getExpression()
                                 ->getChild(1)
                                 ->constCast<binder::LambdaExpression>()
                                 .getParsedLambdaExpr()
                                 ->constCast<parser::ParsedLambdaExpression>()
                                 .getVarNames();
    std::vector<common::idx_t> index(lambdaParamEvaluators.size());
    for (common::idx_t i = 0; i < lambdaParamEvaluators.size(); i++) {
        auto paramName = lambdaParamEvaluators[i]->getVarName();
        auto it = std::find(paramNames.begin(), paramNames.end(), paramName);
        if (it != paramNames.end()) {
            index[i] = it - paramNames.begin();
        } else {
            throw common::RuntimeException(
                common::stringFormat("Lambda paramName {} cannot found.", paramName));
        }
    }
    return index;
}
ListLambdaType ListLambdaEvaluator::checkListLambdaTypeWithFunctionName(std::string functionName) {
    if (0 == functionName.compare(function::ListTransformFunction::name)) {
        return ListLambdaType::LIST_TRANSFORM;
    } else if (0 == functionName.compare(function::ListFilterFunction::name)) {
        return ListLambdaType::LIST_FILTER;
    } else if (0 == functionName.compare(function::ListReduceFunction::name)) {
        return ListLambdaType::LIST_REDUCE;
    } else {
        return ListLambdaType::DEFAULT;
    }
}

} // namespace evaluator
} // namespace kuzu
