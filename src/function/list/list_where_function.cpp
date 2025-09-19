#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "common/types/types.h"
#include "function/list/functions/list_function_utils.h"
#include "function/list/functions/list_position_function.h"
#include "function/list/functions/list_unique_function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListWhere {
    static void operation(common::list_entry_t& left, common::list_entry_t& right,
        common::list_entry_t& result, common::ValueVector& leftVector,
        common::ValueVector& rightVector, common::ValueVector& resultVector) {
        if (right.size!=left.size) {
            throw BinderException(stringFormat("LIST_WHERE expecting lists of same size, receiving size {} and size {}", left.size, left.size));
        }
        auto leftDataVector = common::ListVector::getDataVector(&leftVector);
        auto leftPos = left.offset;
        auto rightDataVector = common::ListVector::getDataVector(&rightVector);
        auto rightPos = right.offset;
        list_size_t resultSize=0;
        std::vector<bool> maskListBools;
        for (auto i=0u; i < right.size; i++) {
            auto maskBool=rightDataVector->getValue<bool>(rightPos+i);
            maskListBools.push_back(maskBool);
            if (maskBool) {
                resultSize++;
            }
        }
        result = common::ListVector::addList(&resultVector, resultSize);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto resultPos = result.offset;
        for (auto i=0u; i < right.size; i++) {
            auto maskBool=maskListBools.at(i);
            if (maskBool) {
                resultDataVector->copyFromVectorData(resultPos++, leftDataVector, leftPos+i);
            }
        }
    }
};
static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());
    if (types[1].getPhysicalType()!=PhysicalTypeID::LIST) {
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(
            ListIntersectFunction::name, types[0].toString(), types[1].toString()));
    } else {
        auto thisExtraTypeInfo=types[1].getExtraTypeInfo();
        auto thisListTypeInfo=ku_dynamic_cast<const ListTypeInfo*>(thisExtraTypeInfo);
        if (thisListTypeInfo->getChildType().getPhysicalType()!=PhysicalTypeID::BOOL) {
            throw BinderException("LIST_SELECT expecting argument type: LIST of ANY, LIST of BOOL");
        }
    }
    return std::make_unique<FunctionBindData>(std::move(types), types[0].copy());
}

function_set ListWhereFunction::getFunctionSet() {
    function_set result;
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, ListWhere>;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::LIST,
        execFunc);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
