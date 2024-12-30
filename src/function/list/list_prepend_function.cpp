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

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    if (input.arguments[0]->getDataType().getLogicalTypeID() != LogicalTypeID::ANY &&
        input.arguments[1]->dataType != ListType::getChildType(input.arguments[0]->dataType)) {
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(
            ListPrependFunction::name, input.arguments[0]->getDataType().toString(),
            input.arguments[1]->getDataType().toString()));
    }
    const auto& resultType = input.arguments[0]->getDataType();
    auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
    TypeUtils::visit(input.arguments[1]->getDataType().getPhysicalType(),
        [&scalarFunction]<typename T>(T) {
            scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, T,
                list_entry_t, ListPrepend>;
        });
    return FunctionBindData::getSimpleBindData(input.arguments, resultType.copy());
}

function_set ListPrependFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST);
    func->bindFunc = bindFunc;
    result.push_back(std::move(func));
    return result;
}

} // namespace function
} // namespace kuzu
