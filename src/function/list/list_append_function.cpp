#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "common/types/types.h"
#include "function/function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListAppend {
    template<typename T>
    static void operation(common::list_entry_t& listEntry, T& value, common::list_entry_t& result,
        common::ValueVector& listVector, common::ValueVector& valueVector,
        common::ValueVector& resultVector) {
        result = common::ListVector::addList(&resultVector, listEntry.size + 1);
        auto listDataVector = common::ListVector::getDataVector(&listVector);
        auto listPos = listEntry.offset;
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto resultPos = result.offset;
        for (auto i = 0u; i < listEntry.size; i++) {
            resultDataVector->copyFromVectorData(resultPos++, listDataVector, listPos++);
        }
        resultDataVector->copyFromVectorData(
            resultDataVector->getData() + resultPos * resultDataVector->getNumBytesPerValue(),
            &valueVector, reinterpret_cast<uint8_t*>(&value));
    }
};


static void resolveEmptyList(const ScalarBindFuncInput& input, std::vector<LogicalType>& types)
{
    auto isEmpty = binder::ExpressionUtil::isEmptyList(*input.arguments[0]);
    if (isEmpty)
    {
        auto elementType = input.arguments[1]->getDataType().copy();

        types[0] = LogicalType::LIST(elementType.copy()).copy();
        types[1] = elementType.copy();
    }
}


static void validateArgumentType(const binder::expression_vector& arguments) {

    if (arguments[0]->getChildren().empty())
        return;

    if (ListType::getChildType(arguments[0]->dataType) != arguments[1]->getDataType()) {
        throw BinderException(
            ExceptionMessage::listFunctionIncompatibleChildrenType(ListAppendFunction::name,
                arguments[0]->getDataType().toString(), arguments[1]->getDataType().toString()));
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    validateArgumentType(input.arguments);

    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());

    resolveEmptyList(input, types);


    if (ListType::getChildType(types[0]) != types[1])
         throw BinderException(
            ExceptionMessage::listFunctionIncompatibleChildrenType(ListAppendFunction::name,
                input.arguments[0]->getDataType().toString(), input.arguments[1]->getDataType().toString()));



    auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
    TypeUtils::visit(input.arguments[1]->getDataType().getPhysicalType(),
        [&scalarFunction]<typename T>(T) {
            scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, T,
                list_entry_t, ListAppend>;
        });
    return FunctionBindData::getSimpleBindData(input.arguments, types[0]);
}

function_set ListAppendFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
