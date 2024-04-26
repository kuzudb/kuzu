#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListReverse {
    static inline void operation(common::list_entry_t& input, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        auto inputDataVector = common::ListVector::getDataVector(&inputVector);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        result = input; // reverse does not change
        for (auto i = 0u; i < input.size; i++) {
            auto pos = input.offset + i;
            auto reversePos = input.offset + input.size - 1 - i;
            resultDataVector->copyFromVectorData(reversePos, inputDataVector, pos);
        }
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    auto resultType = arguments[0]->dataType;
    scalarFunction->execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, ListReverse>;
    return FunctionBindData::getSimpleBindData(arguments, resultType);
}

function_set ListReverseFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST}, LogicalTypeID::ANY, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
