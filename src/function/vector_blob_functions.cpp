#include "function/blob/vector_blob_functions.h"

#include "function/blob/functions/decode_function.h"
#include "function/blob/functions/encode_function.h"
#include "function/blob/functions/octet_length_function.h"
#include "function/string/vector_string_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions OctetLengthVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(OCTET_LENGTH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::BLOB}, LogicalTypeID::INT64,
        UnaryExecFunction<blob_t, int64_t, OctetLength>, nullptr, nullptr, nullptr,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions EncodeVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(ENCODE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::BLOB,
        VectorStringFunction::UnaryStringExecFunction<ku_string_t, blob_t, Encode>, nullptr,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions DecodeVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(DECODE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::BLOB}, LogicalTypeID::STRING,
        VectorStringFunction::UnaryStringExecFunction<blob_t, ku_string_t, Decode>, nullptr,
        false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
