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

struct ListSelect {
    static void operation(common::list_entry_t& left, common::list_entry_t& right,
        common::list_entry_t& result, common::ValueVector& leftVector,
        common::ValueVector& rightVector, common::ValueVector& resultVector) {
        result = common::ListVector::addList(&resultVector, right.size);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto resultPos = result.offset;
        auto leftDataVector = common::ListVector::getDataVector(&leftVector);
        auto rightDataVector = common::ListVector::getDataVector(&rightVector);
        auto rightPos = right.offset;
        for (auto i=0u; i < right.size; i++) {
            auto leftIndexPos=rightDataVector->getValue<long long>(rightPos+i)-1;
            if ((leftIndexPos<0)||(leftIndexPos>=left.size)) {
                // append null to result if out of index
                resultDataVector->setNull(resultPos++, true);
            } else {
                resultDataVector->copyFromVectorData(resultPos++, leftDataVector, leftIndexPos);
            }
        }
    }
};
static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());
    if (types[0].getPhysicalType()!=PhysicalTypeID::LIST) {
        throw BinderException("LIST_SELECT expecting argument type: LIST of ANY, LIST of INT");
    }
    if (types[1].getPhysicalType()!=PhysicalTypeID::LIST) {
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(
            ListIntersectFunction::name, types[0].toString(), types[1].toString()));
    } else {
        auto thisExtraTypeInfo=types[1].getExtraTypeInfo();
        auto thisListTypeInfo=ku_dynamic_cast<const ListTypeInfo*>(thisExtraTypeInfo);
        if (thisListTypeInfo->getChildType().getPhysicalType()!=PhysicalTypeID::INT64) {
            throw BinderException("LIST_SELECT expecting argument type: LIST of ANY, LIST of INT");
        }
    }
    return std::make_unique<FunctionBindData>(std::move(types), types[0].copy());
}

function_set ListSelectFunction::getFunctionSet() {
    function_set result;
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, ListSelect>;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::LIST,
        execFunc);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
