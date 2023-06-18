#include "function/blob/vector_blob_operations.h"

#include "function/blob/operations/decode_operation.h"
#include "function/blob/operations/encode_operation.h"
#include "function/blob/operations/octet_length_operation.h"
#include "function/string/vector_string_operations.h"

namespace kuzu {
namespace function {

vector_operation_definitions OctetLengthVectorOperations::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::OCTET_LENGTH_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::BLOB},
        common::LogicalTypeID::INT64,
        UnaryExecFunction<common::blob_t, int64_t, operation::OctetLength>, nullptr, nullptr,
        nullptr, false /* isVarLength */));
    return definitions;
}

vector_operation_definitions EncodeVectorOperations::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::ENCODE_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
        common::LogicalTypeID::BLOB,
        VectorStringOperations::UnaryStringExecFunction<common::ku_string_t, common::blob_t,
            operation::Encode>,
        nullptr, false /* isVarLength */));
    return definitions;
}

vector_operation_definitions DecodeVectorOperations::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::DECODE_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::BLOB},
        common::LogicalTypeID::STRING,
        VectorStringOperations::UnaryStringExecFunction<common::blob_t, common::ku_string_t,
            operation::Decode>,
        nullptr, false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
