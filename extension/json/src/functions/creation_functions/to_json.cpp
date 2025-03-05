#include "function/scalar_function.h"
#include "json_creation_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

void ToJsonFunction::execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    KU_ASSERT(parameters.size() == 1);
    result.resetAuxiliaryBuffer();
    for (auto i = 0u; i < resultSelVector->getSelSize(); ++i) {
        auto inputPos = (*parameterSelVectors[0])[i];
        auto resultPos = (*resultSelVector)[i];
        result.setNull(resultPos, parameters[0]->isNull(inputPos));
        if (!parameters[0]->isNull(inputPos)) {
            StringVector::addString(&result, resultPos,
                jsonToString(jsonify(*parameters[0], inputPos)));
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
