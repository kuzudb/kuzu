#include "common/exception/binder.h"
#include "function/string/vector_string_functions.h"

namespace kuzu {
namespace function {

using namespace kuzu::common;

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    for (auto i = 0u; i < input.arguments.size(); i++) {
        auto& argument = input.arguments[i];
        if (argument->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            argument->cast(LogicalType::STRING());
        }
        if (argument->getDataType() != LogicalType::STRING()) {
            throw BinderException{stringFormat("concat_ws expects all string parameters. Got: {}.",
                argument->getDataType().toString())};
        }
    }
    return FunctionBindData::getSimpleBindData(input.arguments, LogicalType::STRING());
}

void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {
        auto pos = (*resultSelVector)[selectedPos];
        auto separatorPos = parameters[0]->state->isFlat() ? (*parameterSelVectors[0])[0] : pos;
        if (parameters[0]->isNull(separatorPos)) {
            result.setNull(pos, true /* isNull */);
            continue;
        }
        auto separator = parameters[0]->getValue<ku_string_t>(separatorPos);
        auto len = 0u;
        bool isPrevNull = false;
        for (auto i = 1u; i < parameters.size(); i++) {
            const auto& parameter = parameters[i];
            const auto& parameterSelVector = *parameterSelVectors[i];
            auto paramPos = parameter->state->isFlat() ? parameterSelVector[0] : pos;
            if (parameter->isNull(paramPos)) {
                isPrevNull = true;
                continue;
            }
            if (i != 1u && !isPrevNull) {
                len += separator.len;
            }
            len += parameter->getValue<common::ku_string_t>(paramPos).len;
            isPrevNull = false;
        }
        common::ku_string_t resultStr;
        StringVector::reserveString(&result, resultStr, len);
        auto resultBuffer = resultStr.getData();
        isPrevNull = false;
        for (auto i = 1u; i < parameters.size(); i++) {
            const auto& parameter = parameters[i];
            const auto& parameterSelVector = *parameterSelVectors[i];
            auto paramPos = parameter->state->isFlat() ? parameterSelVector[0] : pos;
            if (parameter->isNull(paramPos)) {
                isPrevNull = true;
                continue;
            }
            auto& str = parameter->getValue<common::ku_string_t>(paramPos);
            if (i != 1u && !isPrevNull) {
                memcpy((void*)resultBuffer, (void*)separator.getData(), separator.len);
                resultBuffer += separator.len;
            }
            memcpy((void*)resultBuffer, (void*)str.getData(), str.len);
            resultBuffer += str.len;
            isPrevNull = false;
        }
        memcpy(resultStr.prefix, resultStr.getData(),
            std::min<uint64_t>(resultStr.len, ku_string_t::PREFIX_LENGTH));
        KU_ASSERT(resultBuffer - resultStr.getData() == len);
        result.setNull(pos, false /* isNull */);
        result.setValue(pos, resultStr);
    }
}

function_set ConcatWSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
        LogicalTypeID::STRING, execFunc);
    func->bindFunc = bindFunc;
    func->isVarLength = true;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
