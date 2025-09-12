#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/scalar_function_expression.h"
#include "binder/expression_binder.h"
#include "function/rewrite_function.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

void PropertiesFunctions::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    result = parameters[0];
}

static std::unique_ptr<Function> getPropertiesFunction(LogicalTypeID logicalTypeID) {
    auto function = std::make_unique<ScalarFunction>(PropertiesFunctions::name,
        std::vector<LogicalTypeID>{logicalTypeID, LogicalTypeID::STRING}, LogicalTypeID::STRUCT);
    function->compileFunc = PropertiesFunctions::compileFunc;
    return function;
}

function_set PropertiesFunctions::getFunctionSet() {
    function_set functions;
    auto inputTypeIDs =
        std::vector<LogicalTypeID>{LogicalTypeID::STRUCT, LogicalTypeID::NODE, LogicalTypeID::REL};
    for (auto inputTypeID : inputTypeIDs) {
        functions.push_back(getPropertiesFunction(inputTypeID));
    }
    return functions;
}

} // namespace function
} // namespace kuzu
