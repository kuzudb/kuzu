#include "function/map/functions/map_extract_function.h"

#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "function/map/vector_map_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static void validateKeyType(const std::shared_ptr<binder::Expression>& mapExpression,
    const std::shared_ptr<binder::Expression>& extractKeyExpression) {
    const auto& mapKeyType = MapType::getKeyType(mapExpression->dataType);
    if (mapKeyType != extractKeyExpression->dataType) {
        throw RuntimeException("Unmatched map key type and extract key type");
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    kuzu::function::Function* function) {
    validateKeyType(arguments[0], arguments[1]);
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    TypeUtils::visit(arguments[1]->getDataType().getPhysicalType(), [&]<typename T>(T) {
        scalarFunction->execFunc =
            ScalarFunction::BinaryExecListStructFunction<list_entry_t, T, list_entry_t, MapExtract>;
    });
    auto resultType = LogicalType::LIST(MapType::getValueType(arguments[0]->dataType).copy());
    return FunctionBindData::getSimpleBindData(arguments, std::move(resultType));
}

function_set MapExtractFunctions::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        nullptr, nullptr, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
