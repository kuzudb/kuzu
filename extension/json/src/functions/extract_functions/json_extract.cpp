#include "common/exception/binder.h"
#include "function/scalar_function.h"
#include "json_extract_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void jsonExtractSinglePath(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    const auto& param1 = parameters[0];
    const auto& param2 = parameters[1];
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
         ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto param1Pos = param1->state->getSelVector()[param1->state->isFlat() ? 0 : selectedPos];
        auto param2Pos = param2->state->getSelVector()[param2->state->isFlat() ? 0 : selectedPos];
        auto isNull = parameters[0]->isNull(param1Pos) || parameters[1]->isNull(param2Pos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            auto param1Str = param1->getValue<ku_string_t>(param1Pos).getAsString();
            std::string output;
            if (param2->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
                auto param2Str = param2->getAsValue(param2Pos)->toString();
                output = jsonExtractToString(stringToJson(param1Str), param2Str);
            } else if (param2->dataType.getLogicalTypeID() == LogicalTypeID::INT64) {
                auto param2Int = param2->getValue<int64_t>(param2Pos);
                output = jsonExtractToString(stringToJson(param1Str), param2Int);
            } else {
                KU_UNREACHABLE;
            }
            StringVector::addString(&result, resultPos, output);
        }
    }
}

static std::unique_ptr<FunctionBindData> bindJsonExtractSinglePath(ScalarBindFuncInput input) {
    KU_ASSERT(input.arguments.size() == 2);
    std::vector<LogicalType> types;
    types.emplace_back(input.definition->parameterTypeIDs[0]);
    types.emplace_back(input.definition->parameterTypeIDs[1]);
    return std::make_unique<FunctionBindData>(std::move(types), JsonType::getJsonType());
}

static void jsonExtractMultiPath(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    const auto resultDataVector = ListVector::getDataVector(&result);
    const auto& param1 = parameters[0];
    const auto& param2 = parameters[1];
    const auto param2DataVector = ListVector::getDataVector(param2.get());
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
         ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto param1Pos = param1->state->getSelVector()[param1->state->isFlat() ? 0 : selectedPos];
        auto param2Pos = param2->state->getSelVector()[param2->state->isFlat() ? 0 : selectedPos];
        auto isNull = parameters[0]->isNull(param1Pos) || parameters[1]->isNull(param2Pos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            auto param1Str = param1->getValue<ku_string_t>(param1Pos).getAsString();
            auto param2List = param2->getValue<list_entry_t>(param2Pos);
            auto resultList = ListVector::addList(&result, param2List.size);
            result.setValue<list_entry_t>(resultPos, resultList);
            for (auto i = 0u; i < resultList.size; ++i) {
                auto curPath =
                    param2DataVector->getValue<ku_string_t>(param2List.offset + i).getAsString();
                resultDataVector->setNull(resultList.offset + i, false);
                StringVector::addString(resultDataVector, resultList.offset + i,
                    jsonExtractToString(stringToJson(param1Str), curPath));
            }
        }
    }
}

static std::unique_ptr<FunctionBindData> bindJsonExtractMultiPath(ScalarBindFuncInput input) {
    KU_ASSERT(input.arguments.size() == 2);
    if (ListType::getChildType(input.arguments[1]->getDataType()).getLogicalTypeID() !=
        LogicalTypeID::STRING) {
        throw BinderException("List passed to json_extract must contain type STRING");
    }
    return FunctionBindData::getSimpleBindData(input.arguments,
        common::LogicalType::LIST(JsonType::getJsonType()));
}

function_set JsonExtractFunction::getFunctionSet() {
    function_set result;
    std::unique_ptr<ScalarFunction> func;
    func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING, jsonExtractSinglePath);
    func->bindFunc = bindJsonExtractSinglePath;
    result.push_back(std::move(func));
    func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING, jsonExtractSinglePath);
    func->bindFunc = bindJsonExtractSinglePath;
    result.push_back(std::move(func));
    func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::LIST}, LogicalTypeID::LIST,
        jsonExtractMultiPath);
    func->bindFunc = bindJsonExtractMultiPath;
    result.push_back(std::move(func));
    return result;
}

} // namespace json_extension
} // namespace kuzu
