#include "common/exception/binder.h"
#include "function/scalar_function.h"
#include "json_creation_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    KU_ASSERT(parameters.size() % 2 == 0);
    result.resetAuxiliaryBuffer();
    for (auto i = 0u; i < result.state->getSelVector().getSelSize(); ++i) {
        auto resultPos = result.state->getSelVector()[i];
        JsonMutWrapper wrapper;
        auto obj = yyjson_mut_obj(wrapper.ptr);
        for (auto j = 0u; j < parameters.size() / 2; j++) {
            auto keyParam = parameters[j * 2];
            if (keyParam->isNull(keyParam->state->isFlat() ? 0 : i)) {
                continue;
            }
            auto valParam = parameters[j * 2 + 1];
            yyjson_mut_obj_add(obj,
                jsonifyAsString(wrapper, *keyParam,
                    keyParam->state->getSelVector()[keyParam->state->isFlat() ? 0 : i]),
                jsonify(wrapper, *valParam,
                    valParam->state->getSelVector()[valParam->state->isFlat() ? 0 : i]));
        }
        yyjson_mut_doc_set_root(wrapper.ptr, obj);
        StringVector::addString(&result, resultPos,
            jsonToString(JsonWrapper(yyjson_mut_doc_imut_copy(wrapper.ptr, nullptr /* alc */))));
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    if (input.arguments.size() % 2 != 0) {
        throw common::BinderException{"json_object() requires an even number of arguments"};
    }
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

function_set JsonObjectFunction::getFunctionSet() {
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
