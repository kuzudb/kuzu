#include "common/type_utils.h"
#include "function/list/functions/list_position_function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListContains {
    template<typename T>
    static void operation(common::list_entry_t& list, T& element, uint8_t& result,
        common::ValueVector& listVector, common::ValueVector& elementVector,
        common::ValueVector& resultVector) {
        int64_t pos;
        ListPosition::operation(list, element, pos, listVector, elementVector, resultVector);
        result = (pos != 0);
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
    TypeUtils::visit(input.arguments[1]->getDataType().getPhysicalType(),
        [&scalarFunction]<typename T>(T) {
            scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, T,
                uint8_t, ListContains>;
        });
    return FunctionBindData::getSimpleBindData(input.arguments, LogicalType::BOOL());
}

function_set ListContainsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::BOOL,
        nullptr, nullptr, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
