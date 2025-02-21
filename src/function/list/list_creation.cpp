#include "binder/expression/expression_util.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void ListCreationFunction::execFunc(std::span<const common::SelectedVector> parameters,
    common::SelectedVector result, void* /*dataPtr*/) {
    result.vec.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < result.sel->getSelSize(); ++selectedPos) {
        auto pos = (*result.sel)[selectedPos];
        auto resultEntry = ListVector::addList(&result.vec, parameters.size());
        result.vec.setValue(pos, resultEntry);
        auto resultDataVector = ListVector::getDataVector(&result.vec);
        auto resultPos = resultEntry.offset;
        for (auto i = 0u; i < parameters.size(); i++) {
            const auto& parameter = parameters[i];
            auto paramPos = parameter.vec.state->isFlat() ? (*parameter.sel)[0] : pos;
            resultDataVector->copyFromVectorData(resultPos++, &parameter.vec, paramPos);
        }
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    LogicalType combinedType(LogicalTypeID::ANY);
    binder::ExpressionUtil::tryCombineDataType(input.arguments, combinedType);
    if (combinedType.getLogicalTypeID() == LogicalTypeID::ANY) {
        combinedType = LogicalType::INT64();
    }
    auto resultType = LogicalType::LIST(combinedType.copy());
    auto bindData = std::make_unique<FunctionBindData>(std::move(resultType));
    for (auto& _ : input.arguments) {
        (void)_;
        bindData->paramTypes.push_back(combinedType.copy());
    }
    return bindData;
}

function_set ListCreationFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::LIST, execFunc);
    function->bindFunc = bindFunc;
    function->isVarLength = true;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
