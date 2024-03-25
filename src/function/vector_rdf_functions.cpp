#include "function/rdf/vector_rdf_functions.h"

#include "function/rdf/rdf_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set RDFTypeFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::RDF_VARIANT}, LogicalTypeID::STRING,
        ScalarFunction::UnaryExecNestedTypeFunction<struct_entry_t, ku_string_t, Type>));
    return result;
}

function_set ValidatePredicateFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::STRING,
        ScalarFunction::UnaryStringExecFunction<ku_string_t, ku_string_t, ValidatePredicate>));
    return result;
}

} // namespace function
} // namespace kuzu
