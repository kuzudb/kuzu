#include "function/scalar_function.h"
#include "function/utility/function_string_bind_data.h"
#include "function/utility/vector_utility_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    std::unique_ptr<FunctionBindData> bindData;
    if (arguments[0]->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
        bindData = std::make_unique<FunctionStringBindData>("NULL");
        bindData->paramTypes.push_back(LogicalType::STRING());
    } else {
        bindData = std::make_unique<FunctionStringBindData>(arguments[0]->getDataType().toString());
    }
    return bindData;
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>&, ValueVector& result,
    void* dataPtr) {
    result.resetAuxiliaryBuffer();
    auto typeData = reinterpret_cast<FunctionStringBindData*>(dataPtr);
    for (auto i = 0u; i < result.state->getSelVector().getSelSize(); ++i) {
        auto resultPos = result.state->getSelVector()[i];
        StringVector::addString(&result, resultPos, typeData->str);
    }
}

function_set TypeOfFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::STRING, execFunc, nullptr /* selectFunc */, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
