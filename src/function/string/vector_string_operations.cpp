#include "include/vector_string_operations.h"

#include "operations/include/concat_operation.h"
#include "operations/include/contains_operation.h"
#include "operations/include/length_operation.h"
#include "operations/include/repeat_operation.h"
#include "operations/include/starts_with_operation.h"

namespace graphflow {
namespace function {

vector<unique_ptr<VectorOperationDefinition>> ContainsVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    auto execFunc = BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::Contains>;
    auto selectFunc = BinarySelectFunction<gf_string_t, gf_string_t, operation::Contains>;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONTAINS_FUNC_NAME,
        vector<DataTypeID>{STRING, STRING}, BOOL, execFunc, selectFunc, false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> StartsWithVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    auto execFunc = BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::StartsWith>;
    auto selectFunc = BinarySelectFunction<gf_string_t, gf_string_t, operation::StartsWith>;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(STARTS_WITH_FUNC_NAME,
        vector<DataTypeID>{STRING, STRING}, BOOL, execFunc, selectFunc, false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> ConcatVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    auto execFunc = BinaryExecFunction<gf_string_t, gf_string_t, gf_string_t, operation::Concat>;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONCAT_FUNC_NAME,
        vector<DataTypeID>{STRING, STRING}, STRING, execFunc, false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> LengthVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    auto execFunc = UnaryExecFunction<gf_string_t, int64_t, operation::Length>;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(
        LENGTH_FUNC_NAME, vector<DataTypeID>{STRING}, INT64, execFunc, false /* isVarLength */));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> RepeatVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    auto execFunc = BinaryExecFunction<gf_string_t, int64_t, gf_string_t, operation::Repeat>;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REPEAT_FUNC_NAME,
        vector<DataTypeID>{STRING, INT64}, STRING, execFunc, false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace graphflow
