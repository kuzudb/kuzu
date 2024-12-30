#include "binder/expression/expression_util.h"
#include "common/type_utils.h"
#include "function/list/functions/list_position_function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

struct ListContains {
    template<typename T>
    static void operation(common::list_entry_t& list, T& element, uint8_t& result,
        common::ValueVector& listVector, common::ValueVector& elementVector,
        common::ValueVector& resultVector) {
        int64_t pos = 0;
        ListPosition::operation(list, element, pos, listVector, elementVector, resultVector);
        result = (pos != 0);
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
    TypeUtils::visit(ListType::getChildType(input.arguments[0]->getDataType()).getPhysicalType(),
        [&scalarFunction]<typename T>(T) {
            scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, T,
                uint8_t, ListContains>;
        });
    // for list_contains(list, input), we expect input and list child have the same type, if list
    // is empty, we use in the input type. Otherwise, we use list child type because casting list
    // is more expensive.
    std::vector<LogicalType> paramTypes;
    if (ExpressionUtil::isEmptyList(*input.arguments[0])) {
        auto& inputType = input.arguments[1]->getDataType();
        paramTypes.push_back(LogicalType::LIST(inputType.copy()));
        paramTypes.push_back(inputType.copy());
    } else {
        auto& listType = input.arguments[0]->getDataType();
        paramTypes.push_back(listType.copy());
        paramTypes.push_back(ListType::getChildType(listType).copy());
    }
    return std::make_unique<FunctionBindData>(std::move(paramTypes), LogicalType::BOOL());
}

function_set ListContainsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::BOOL);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
