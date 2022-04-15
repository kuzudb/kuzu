#include "include/vector_string_operations.h"

#include "operations/include/concat_operation.h"
#include "operations/include/contains_operation.h"
#include "operations/include/starts_with_operation.h"

namespace graphflow {
namespace function {

vector<unique_ptr<VectorOperationDefinition>> ContainsVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    auto execFunc = BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::Contains>;
    auto selectFunc = BinarySelectFunction<gf_string_t, gf_string_t, operation::Contains>;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(
        CONTAINS_FUNC_NAME, vector<DataTypeID>{STRING, STRING}, BOOL, execFunc, selectFunc, false));
    return definitions;
}

vector<unique_ptr<VectorOperationDefinition>> StartsWithVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    auto execFunc = BinaryExecFunction<gf_string_t, gf_string_t, uint8_t, operation::StartsWith>;
    auto selectFunc = BinarySelectFunction<gf_string_t, gf_string_t, operation::StartsWith>;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(STARTS_WITH_FUNC_NAME,
        vector<DataTypeID>{STRING, STRING}, BOOL, execFunc, selectFunc, false));
    return definitions;
}

} // namespace function
} // namespace graphflow
