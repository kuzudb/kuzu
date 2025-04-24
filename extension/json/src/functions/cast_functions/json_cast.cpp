#include "common/exception/conversion.h"
#include "common/json_common.h"
#include "common/string_format.h"
#include "function/scalar_function.h"
#include "json_cast_functions.h"
#include "json_creation_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& /*parameterSelVectors*/,
    common::ValueVector& result, common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    for (auto i = 0u; i < resultSelVector->getSelSize(); ++i) {
        auto resultPos = (*resultSelVector)[i];
        auto str = parameters[0]->getValue<common::ku_string_t>(resultPos);
        yyjson_read_err error;
        auto doc = JSONCommon::readDocumentUnsafe(str.getDataUnsafe(), str.len,
            JSONCommon::READ_FLAG, nullptr /* alc */, &error);
        if (!doc) {
            yyjson_doc_free(doc);
            throw common::ConversionException{
                common::stringFormat("Cannot convert {} to JSON.", str.getAsString())};
        }
        yyjson_doc_free(doc);
        StringVector::addString(&result, resultPos, str);
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

function_set CastToJsonFunction::getFunctionSet() {
    function_set result;
    auto function =
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::STRING, ToJsonFunction::execFunc);
    function->bindFunc = bindFunc;
    result.push_back(function->copy());
    function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::STRING, execFunc);
    result.push_back(std::move(function));
    return result;
}

} // namespace json_extension
} // namespace kuzu
