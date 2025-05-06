#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "common/types/types.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"
#include "binder/expression/expression_util.h"

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
    
    if (arguments[0]->getDataType().getLogicalTypeID() != LogicalTypeID::ANY &&
        arguments[1]->dataType != ListType::getChildType(arguments[0]->dataType)) {
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(
            ListPrependFunction::name, arguments[0]->getDataType().toString(),
            arguments[1]->getDataType().toString()));
    }
}


static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    validateArgumentType(input.arguments);

    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());

    resolveEmptyList(input, types);

    if (types[0].getLogicalTypeID() != LogicalTypeID::ANY &&
        types[1] != ListType::getChildType(types[0]))
            throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType
            (
                ListPrependFunction::name,
                types[0].toString(),
                types[1].toString()
            ));

    auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
    TypeUtils::visit(types[1].getPhysicalType(),
        [&scalarFunction]<typename T>(T) {
            scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, T,
                list_entry_t, ListPrepend>;
        });
    return FunctionBindData::getSimpleBindData(input.arguments, types[0]);
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
