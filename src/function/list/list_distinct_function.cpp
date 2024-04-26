#include "function/list/functions/list_unique_function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListDistinct {
    static void operation(common::list_entry_t& input, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        auto numUniqueValues = ListUnique::appendListElementsToValueSet(input, inputVector);
        result = common::ListVector::addList(&resultVector, numUniqueValues);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto resultDataVectorBuffer =
            common::ListVector::getListValuesWithOffset(&resultVector, result, 0 /* offset */);
        ListUnique::appendListElementsToValueSet(input, inputVector, nullptr,
            [&resultDataVector, &resultDataVectorBuffer](common::ValueVector& dataVector,
                uint64_t pos) -> void {
                resultDataVector->copyFromVectorData(resultDataVectorBuffer, &dataVector,
                    dataVector.getData() + pos * dataVector.getNumBytesPerValue());
                resultDataVectorBuffer += dataVector.getNumBytesPerValue();
            });
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function*) {
    return FunctionBindData::getSimpleBindData(arguments, arguments[0]->getDataType());
}

function_set ListDistinctFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST}, LogicalTypeID::LIST,
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, ListDistinct>,
        nullptr, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
