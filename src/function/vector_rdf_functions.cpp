#include "function/rdf/vector_rdf_functions.h"

#include "function/rdf/rdf_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set RDFTypeFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(TYPE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RDF_VARIANT}, LogicalTypeID::STRING,
        ScalarFunction::UnaryExecNestedTypeFunction<struct_entry_t, ku_string_t, Type>));
    return result;
}

} // namespace function
} // namespace kuzu
