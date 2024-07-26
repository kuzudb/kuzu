#include "binder/expression/expression_util.h"
#include "function/path/vector_path_functions.h"
#include "function/scalar_function.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    const auto& structType = input.arguments[0]->getDataType();
    auto fieldIdx = StructType::getFieldIdx(structType, InternalKeyword::NODES);
    auto resultType = StructType::getField(structType, fieldIdx).getType().copy();
    auto bindData = std::make_unique<StructExtractBindData>(std::move(resultType), fieldIdx);
    bindData->paramTypes = ExpressionUtil::getDataTypes(input.arguments);
    return bindData;
}

function_set NodesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL},
            LogicalTypeID::ANY, nullptr, nullptr, StructExtractFunctions::compileFunc, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
