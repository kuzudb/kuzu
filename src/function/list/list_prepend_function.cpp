#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListPrepend {
    template<typename T>
    static void operation(common::list_entry_t& listEntry, T& value, common::list_entry_t& result,
        common::ValueVector& listVector, common::ValueVector& valueVector,
        common::ValueVector& resultVector) {
        result = common::ListVector::addList(&resultVector, listEntry.size + 1);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        resultDataVector->copyFromVectorData(
            common::ListVector::getListValues(&resultVector, result), &valueVector,
            reinterpret_cast<uint8_t*>(&value));
        auto resultPos = result.offset + 1;
        auto listDataVector = common::ListVector::getDataVector(&listVector);
        auto listPos = listEntry.offset;
        for (auto i = 0u; i < listEntry.size; i++) {
            resultDataVector->copyFromVectorData(resultPos++, listDataVector, listPos++);
        }
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    if (arguments[0]->getDataType().getLogicalTypeID() != LogicalTypeID::ANY &&
        arguments[1]->dataType != ListType::getChildType(arguments[0]->dataType)) {
        throw BinderException(
            ExceptionMessage::listFunctionIncompatibleChildrenType(ListPrependFunction::name,
                arguments[0]->getDataType().toString(), arguments[1]->getDataType().toString()));
    }
    const auto& resultType = arguments[0]->getDataType();
    auto scalarFunction = function->ptrCast<ScalarFunction>();
    TypeUtils::visit(arguments[1]->getDataType().getPhysicalType(),
        [&scalarFunction]<typename T>(T) {
            scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, T,
                list_entry_t, ListPrepend>;
        });
    return FunctionBindData::getSimpleBindData(arguments, resultType.copy());
}

function_set ListPrependFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
