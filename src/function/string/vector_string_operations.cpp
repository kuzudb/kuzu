#include "include/vector_string_operations.h"

#include "operations/include/array_extract_operation.h"
#include "operations/include/concat_operation.h"
#include "operations/include/contains_operation.h"
#include "operations/include/ends_with_operation.h"
#include "operations/include/left_operation.h"
#include "operations/include/length_operation.h"
#include "operations/include/lpad_operation.h"
#include "operations/include/repeat_operation.h"
#include "operations/include/right_operation.h"
#include "operations/include/rpad_operation.h"
#include "operations/include/starts_with_operation.h"
#include "operations/include/substr_operation.h"

namespace graphflow {
namespace function {

vector<unique_ptr<VectorOperationDefinition>> ContainsVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONTAINS_FUNC_NAME,
        vector<DataTypeID>{STRING, STRING}, BOOL,
        BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::Contains>,
        BinarySelectFunction<gf_string_t, gf_string_t, operation::Contains>,
        false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> StartsWithVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(STARTS_WITH_FUNC_NAME,
        vector<DataTypeID>{STRING, STRING}, BOOL,
        BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::StartsWith>,
        BinarySelectFunction<gf_string_t, gf_string_t, operation::StartsWith>,
        false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> EndsWithVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(ENDS_WITH_FUNC_NAME,
        vector<DataTypeID>{STRING, STRING}, BOOL,
        BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::EndsWith>,
        BinarySelectFunction<gf_string_t, gf_string_t, operation::EndsWith>,
        false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> ConcatVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONCAT_FUNC_NAME,
        vector<DataTypeID>{STRING, STRING}, STRING,
        BinaryStringExecFunction<gf_string_t, gf_string_t, gf_string_t, operation::Concat>,
        false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> LengthVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(
        make_unique<VectorOperationDefinition>(LENGTH_FUNC_NAME, vector<DataTypeID>{STRING}, INT64,
            UnaryExecFunction<gf_string_t, int64_t, operation::Length>, false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> RepeatVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(
        make_unique<VectorOperationDefinition>(REPEAT_FUNC_NAME, vector<DataTypeID>{STRING, INT64},
            STRING, BinaryStringExecFunction<gf_string_t, int64_t, gf_string_t, operation::Repeat>,
            false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> LpadVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LPAD_FUNC_NAME,
        vector<DataTypeID>{STRING, INT64, STRING}, STRING,
        TernaryStringExecFunction<gf_string_t, int64_t, gf_string_t, gf_string_t, operation::Lpad>,
        false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> RpadVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(RPAD_FUNC_NAME,
        vector<DataTypeID>{STRING, INT64, STRING}, STRING,
        TernaryStringExecFunction<gf_string_t, int64_t, gf_string_t, gf_string_t, operation::Rpad>,
        false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> SubStrVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(SUBSTRING_FUNC_NAME,
        vector<DataTypeID>{STRING, INT64, INT64}, STRING,
        TernaryStringExecFunction<gf_string_t, int64_t, int64_t, gf_string_t, operation::SubStr>,
        false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> LeftVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(
        make_unique<VectorOperationDefinition>(LEFT_FUNC_NAME, vector<DataTypeID>{STRING, INT64},
            STRING, BinaryStringExecFunction<gf_string_t, int64_t, gf_string_t, operation::Left>,
            false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> RightVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(
        make_unique<VectorOperationDefinition>(RIGHT_FUNC_NAME, vector<DataTypeID>{STRING, INT64},
            STRING, BinaryStringExecFunction<gf_string_t, int64_t, gf_string_t, operation::Right>,
            false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> ArrayExtractVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(ARRAY_EXTRACT_FUNC_NAME,
        vector<DataTypeID>{STRING, INT64}, STRING,
        BinaryExecFunction<gf_string_t, int64_t, gf_string_t, operation::ArrayExtract>,
        false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace graphflow
