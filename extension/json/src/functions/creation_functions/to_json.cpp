#include "function/scalar_function.h"
#include "json_creation_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(std::span<const common::SelectedVector> parameters,
    common::SelectedVector result, void* /*dataPtr*/) {
    KU_ASSERT(parameters.size() == 1);
    result.vec.resetAuxiliaryBuffer();
    for (auto i = 0u; i < result.sel.getSelSize(); ++i) {
        auto inputPos = parameters[0].sel[i];
        auto resultPos = result.sel[i];
        result.vec.setNull(resultPos, parameters[0].vec.isNull(inputPos));
        if (!parameters[0].vec.isNull(inputPos)) {
            StringVector::addString(&result.vec, resultPos,
                jsonToString(jsonify(parameters[0].vec, inputPos)));
        }
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    LogicalType type = input.arguments[0]->getDataType().copy();
    if (type.getLogicalTypeID() == LogicalTypeID::ANY) {
        type = LogicalType::INT64();
    }
    auto bindData = std::make_unique<FunctionBindData>(JsonType::getJsonType());
    bindData->paramTypes.push_back(std::move(type));
    return bindData;
}

function_set ToJsonFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::STRING, execFunc);
    func->bindFunc = bindFunc;
    result.push_back(std::move(func));
    return result;
}

} // namespace json_extension
} // namespace kuzu
