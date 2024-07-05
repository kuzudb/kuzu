#include "common/exception/binder.h"
#include "expression_evaluator/lambda_evaluator.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

using namespace kuzu::common;

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    if (arguments[1]->expressionType != ExpressionType::LAMBDA) {
        throw BinderException(stringFormat(
            "The second argument of LIST_FILTER should be a lambda expression but got {}.",
            ExpressionTypeUtil::toString(arguments[1]->expressionType)));
    }
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType().copy());
    paramTypes.push_back(arguments[1]->getDataType().copy());
    if (arguments[1]->getDataType() != LogicalType::BOOL()) {
        throw BinderException(stringFormat(
            "{} requires the result type of lambda expression be BOOL.", ListFilterFunction::name));
    }
    return std::make_unique<FunctionBindData>(std::move(paramTypes),
        LogicalType::LIST(ListType::getChildType(arguments[0]->getDataType()).copy()));
}

static uint64_t getSelectedListSize(const list_entry_t& srcListEntry,
    const ValueVector& filterVector) {
    uint64_t counter = 0;
    for (auto j = 0u; j < srcListEntry.size; j++) {
        auto pos = srcListEntry.offset + j;
        if (filterVector.getValue<bool>(pos)) {
            counter++;
        }
    }
    return counter;
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& input, ValueVector& result,
    void* bindData) {
    auto listLambdaBindData = reinterpret_cast<evaluator::ListLambdaBindData*>(bindData);
    auto inputVector = input[0].get();
    auto listSize = ListVector::getDataVectorSize(inputVector);
    auto lambdaParamVector = listLambdaBindData->lambdaParamEvaluators[0]->resultVector.get();
    lambdaParamVector->state->getSelVectorUnsafe().setSelSize(listSize);
    listLambdaBindData->rootEvaluator->evaluate();
    KU_ASSERT(input.size() == 2);
    auto& listInputSelVector = input[0]->state->getSelVector();
    auto filterVector = input[1].get();
    auto srcDataVector = ListVector::getDataVector(inputVector);
    auto dstDataVector = ListVector::getDataVector(&result);
    for (auto i = 0u; i < listInputSelVector.getSelSize(); ++i) {
        auto srcListEntry = inputVector->getValue<list_entry_t>(listInputSelVector[i]);
        auto dstListEntry =
            ListVector::addList(&result, getSelectedListSize(srcListEntry, *filterVector));
        auto dstListOffset = dstListEntry.offset;
        for (auto j = 0u; j < srcListEntry.size; j++) {
            auto pos = srcListEntry.offset + j;
            if (filterVector->getValue<bool>(pos)) {
                dstDataVector->copyFromVectorData(dstListOffset, srcDataVector, pos);
                dstListOffset++;
            }
        }
        result.setValue(result.state->getSelVector()[i], std::move(dstListEntry));
    }
}

function_set ListFilterFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        execFunc, bindFunc);
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
