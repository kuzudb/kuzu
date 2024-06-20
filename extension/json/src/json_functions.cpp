#include "json_functions.h"

#include "common/exception/binder.h"
#include "function/scalar_function.h"
#include "json_utils.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace json_extension {

static void toJsonExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    KU_ASSERT(parameters.size() == 1);
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto inputPos = parameters[0]->state->getSelVector()[selectedPos];
        auto resultPos = result.state->getSelVector()[selectedPos];
        StringVector::addString(&result, resultPos, jsonToString(jsonify(*parameters[0], inputPos)));
    }
}

function_set ToJsonFunction::getFunctionSet() {
    function_set ret;
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
        LogicalTypeID::STRING, toJsonExecFunc));
    return ret;
}

static void jsonMergeFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    const auto& param1 = parameters[0];
    const auto& param2 = parameters[1];
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto param1Pos = param1->state->getSelVector()[param1->state->isFlat() ? 0 : selectedPos];
        auto param2Pos = param2->state->getSelVector()[param2->state->isFlat() ? 0 : selectedPos];
        auto param1Str = param1->getValue<ku_string_t>(param1Pos).getAsString();
        auto param2Str = param2->getValue<ku_string_t>(param2Pos).getAsString();
        StringVector::addString(&result, resultPos,
            jsonToString(mergeJson(stringToJson(param1Str), stringToJson(param2Str))));
    }
}

function_set JsonMergeFunction::getFunctionSet() {
    function_set ret;
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING,
        LogicalTypeID::STRING}, LogicalTypeID::STRING, jsonMergeFunc));
    return ret;
}

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
        auto param1Str = param1->getValue<ku_string_t>(param1Pos).getAsString();
        auto param2List = param2->getValue<list_entry_t>(param2Pos);
        auto resultList = ListVector::addList(&result, param2List.size);
        resultDataVector->setValue<list_entry_t>(resultPos, resultList);
        for (auto i = 0u; i < resultList.size; ++i) {
            auto curPath = param2DataVector->getValue<ku_string_t>(param2List.offset + i).getAsString();
            StringVector::addString(resultDataVector, resultList.offset + i,
                jsonExtractToString(stringToJson(param1Str), curPath));
        }
    }
}

static std::unique_ptr<FunctionBindData> bindJsonExtractMultiPath(
    const binder::expression_vector& params, Function* /*definition*/) {
    KU_ASSERT(params.size() == 2);
    if (params[1]->getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
        throw BinderException("List passed to json_extract must contain type STRING");
    }
    return FunctionBindData::getSimpleBindData(params, LogicalType::LIST(LogicalType::STRING()));
}

function_set JsonExtractFunction::getFunctionSet() {
    function_set ret;
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING,
        LogicalTypeID::STRING}, LogicalTypeID::STRING, jsonExtractSinglePath));
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING,
        LogicalTypeID::INT64}, LogicalTypeID::STRING, jsonExtractSinglePath));
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING,
        LogicalTypeID::LIST}, LogicalTypeID::LIST, jsonExtractMultiPath, nullptr, bindJsonExtractMultiPath));
    return ret;
}

static void jsonArrayLength(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    KU_ASSERT(parameters.size() == 1);
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto inputPos = parameters[0]->state->getSelVector()[selectedPos];
        auto resultPos = result.state->getSelVector()[selectedPos];
        result.setValue<uint32_t>(resultPos, jsonArraySize(
            stringToJson(parameters[0]->getValue<ku_string_t>(inputPos).getAsString())));
    }
}

function_set JsonArrayLengthFunction::getFunctionSet() {
    function_set ret;
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING,
        LogicalTypeID::STRING}, LogicalTypeID::UINT32, jsonExtractSinglePath));
    return ret;
}

static void jsonContainsExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    const auto& param1 = parameters[0];
    const auto& param2 = parameters[1];
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto param1Pos = param1->state->getSelVector()[param1->state->isFlat() ? 0 : selectedPos];
        auto param2Pos = param2->state->getSelVector()[param2->state->isFlat() ? 0 : selectedPos];
        auto param1Str = param1->getValue<ku_string_t>(param1Pos).getAsString();
        auto param2Str = param2->getValue<ku_string_t>(param2Pos).getAsString();
        result.setValue<bool>(resultPos, jsonContains(stringToJson(param1Str), stringToJson(param2Str)));
    }
}

function_set JsonContainsFunction::getFunctionSet() {
    function_set ret;
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING,
        LogicalTypeID::STRING}, LogicalTypeID::BOOL, jsonContainsExecFunc));
    return ret;
}

static void jsonKeysExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    auto resultDataVector = ListVector::getDataVector(&result);
    resultDataVector->resetAuxiliaryBuffer();
    const auto& param = parameters[0];
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto paramPos = param->state->getSelVector()[param->state->isFlat() ? 0 : selectedPos];
        auto paramStr = param->getValue<ku_string_t>(paramPos).getAsString();
        auto keys = jsonGetKeys(stringToJson(paramStr));
        auto resultList = ListVector::addList(&result, keys.size());
        resultDataVector->setValue<list_entry_t>(resultPos, resultList);
        for (auto i = 0u; i < resultList.size; ++i) {
            StringVector::addString(resultDataVector, resultList.offset + i,
                jsonExtractToString(stringToJson(paramStr), keys[i]));
        }
    }
}

function_set JsonKeysFunction::getFunctionSet() {
    function_set ret;
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
        LogicalTypeID::LIST, jsonKeysExecFunc));
    return ret;
}

function_set JsonStructureFunction::getFunctionSet() {
    function_set ret;
    // ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
    //     LogicalTypeID::ANY)); // how is the output type supposed to work here? todo.
    return ret;
}

static void jsonValidExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto inputPos = parameters[0]->state->getSelVector()[selectedPos];
        auto resultPos = result.state->getSelVector()[selectedPos];
        result.setValue<bool>(resultPos,
            stringToJson(parameters[0]->getValue<ku_string_t>(inputPos).getAsString()).ptr != nullptr);
    }
}

function_set JsonValidFunction::getFunctionSet() {
    function_set ret;
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
        LogicalTypeID::BOOL, jsonValidExecFunc));
    return ret;
}

static void jsonMinifyExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto inputPos = parameters[0]->state->getSelVector()[selectedPos];
        auto resultPos = result.state->getSelVector()[selectedPos];
        StringVector::addString(&result, resultPos,
            jsonToString(stringToJson(parameters[0]->getValue<ku_string_t>(inputPos).getAsString())));
    }
}

function_set MinifyJsonFunction::getFunctionSet() {
    function_set ret;
    ret.push_back(std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
        LogicalTypeID::BOOL, jsonMinifyExecFunc));
    return ret;
}

}
}
