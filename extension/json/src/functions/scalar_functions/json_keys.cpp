#include "common/json_common.h"
#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(std::span<const common::SelectedVector> parameters,
    common::SelectedVector result, void* /*dataPtr*/) {
    result.vec.resetAuxiliaryBuffer();
    auto resultDataVector = ListVector::getDataVector(&result.vec);
    resultDataVector->resetAuxiliaryBuffer();
    const auto& param = parameters[0];
    size_t idx = 0, max = 0;
    yyjson_val *key = nullptr, *childVal = nullptr;
    for (auto selectedPos = 0u; selectedPos < result.sel->getSelSize(); ++selectedPos) {
        auto resultPos = (*result.sel)[selectedPos];
        auto paramPos = (*param.sel)[param.vec.state->isFlat() ? 0 : selectedPos];
        auto isNull = param.vec.isNull(paramPos);
        result.vec.setNull(resultPos, isNull);
        if (!isNull) {
            auto paramStr = param.vec.getValue<ku_string_t>(paramPos).getAsString();
            auto doc = JsonWrapper{JSONCommon::readDocument(paramStr, JSONCommon::READ_FLAG)};
            auto numKeys = yyjson_obj_size(yyjson_doc_get_root(doc.ptr));
            auto resultList = ListVector::addList(&result.vec, numKeys);
            result.vec.setValue<list_entry_t>(resultPos, resultList);
            yyjson_obj_foreach(yyjson_doc_get_root(doc.ptr), idx, max, key, childVal) {
                StringVector::addString(resultDataVector, resultList.offset + idx,
                    std::string(unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key)));
            }
        }
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    KU_ASSERT(input.arguments.size() == 1);
    auto type = input.arguments[0]->dataType.copy();
    if (type.getLogicalTypeID() == LogicalTypeID::ANY) {
        type = LogicalType::INT64();
    }
    auto bindData = std::make_unique<FunctionBindData>(LogicalType::LIST(LogicalType::STRING()));
    bindData->paramTypes.push_back(std::move(type));
    return bindData;
}

function_set JsonKeysFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::LIST, execFunc);
    func->bindFunc = bindFunc;
    result.push_back(std::move(func));
    return result;
}

} // namespace json_extension
} // namespace kuzu
