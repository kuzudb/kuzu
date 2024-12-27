#include "common/vector/value_vector.h"
#include "function/path/path_function_executor.h"
#include "function/path/vector_path_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    return FunctionBindData::getSimpleBindData(input.arguments, LogicalType::BOOL());
}

static void IsTrailExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    UnaryPathExecutor::executeRelIDs(*parameters[0], result);
}

static bool IsTrailSelectFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    SelectionVector& selectionVector) {
    return UnaryPathExecutor::selectRelIDs(*parameters[0], selectionVector);
}

function_set IsTrailFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::BOOL,
        IsTrailExecFunc, IsTrailSelectFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));
    return functionSet;
}

static void IsACyclicExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    UnaryPathExecutor::executeNodeIDs(*parameters[0], result);
}

static bool IsACyclicSelectFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    SelectionVector& selectionVector) {
    return UnaryPathExecutor::selectNodeIDs(*parameters[0], selectionVector);
}

function_set IsACyclicFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::BOOL,
        IsACyclicExecFunc, IsACyclicSelectFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
