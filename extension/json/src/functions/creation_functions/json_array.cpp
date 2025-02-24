#include "function/scalar_function.h"
#include "json_creation_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    for (auto i = 0u; i < resultSelVector->getSelSize(); ++i) {
        auto resultPos = (*resultSelVector)[i];
        JsonMutWrapper wrapper;
        auto mutArray = yyjson_mut_arr(wrapper.ptr);
        for (size_t j = 0; j < parameters.size(); ++j) {
            const auto& param = *parameters[j];
            const auto& paramSelVector = *parameterSelVectors[j];
            auto paramPos = param.state->isFlat() ? paramSelVector[0] : paramSelVector[i];
            yyjson_mut_arr_append(mutArray, jsonify(wrapper, param, paramPos));
        }
        yyjson_mut_doc_set_root(wrapper.ptr, mutArray);
        StringVector::addString(&result, resultPos,
            jsonToString(JsonWrapper(yyjson_mut_doc_imut_copy(wrapper.ptr, nullptr /* alc */))));
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    auto bindData = std::make_unique<FunctionBindData>(JsonType::getJsonType());
    for (auto& arg : input.arguments) {
        LogicalType type = arg->dataType.copy();
        if (type.getLogicalTypeID() == LogicalTypeID::ANY) {
            type = LogicalType::INT64();
        }
        bindData->paramTypes.push_back(std::move(type));
    }
    return bindData;
}

function_set JsonArrayFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::STRING, execFunc);
    function->bindFunc = bindFunc;
    function->isVarLength = true;
    result.push_back(std::move(function));
    return result;
}

} // namespace json_extension
} // namespace kuzu
