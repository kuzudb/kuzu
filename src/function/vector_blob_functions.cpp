#include "function/blob/vector_blob_functions.h"

#include "function/blob/functions/decode_function.h"
#include "function/blob/functions/encode_function.h"
#include "function/blob/functions/octet_length_function.h"
#include "function/string/vector_string_functions.h"

namespace kuzu {
namespace function {

vector_function_definitions OctetLengthVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::OCTET_LENGTH_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::BLOB},
        common::LogicalTypeID::INT64, UnaryExecFunction<common::blob_t, int64_t, OctetLength>,
        nullptr, nullptr, nullptr, false /* isVarLength */));
    return definitions;
}

vector_function_definitions EncodeVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::ENCODE_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
        common::LogicalTypeID::BLOB,
        VectorStringFunction::UnaryStringExecFunction<common::ku_string_t, common::blob_t, Encode>,
        nullptr, false /* isVarLength */));
    return definitions;
}

vector_function_definitions DecodeVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::DECODE_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::BLOB},
        common::LogicalTypeID::STRING,
        VectorStringFunction::UnaryStringExecFunction<common::blob_t, common::ku_string_t, Decode>,
        nullptr, false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
