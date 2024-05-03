#include "binder/expression/expression_util.h"
#include "function/path/vector_path_functions.h"
#include "function/scalar_function.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function*) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = StructType::getFieldIdx(structType, InternalKeyword::RELS);
    auto resultType = StructType::getFieldTypes(structType)[fieldIdx];
    auto bindData = std::make_unique<StructExtractBindData>(resultType.copy(), fieldIdx);
    bindData->paramTypes = binder::ExpressionUtil::getDataTypes(arguments);
    return bindData;
}

function_set RelsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL},
            LogicalTypeID::ANY, nullptr, nullptr, StructExtractFunctions::compileFunc, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
