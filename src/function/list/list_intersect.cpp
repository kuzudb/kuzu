#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "function/list/functions/list_function_utils.h"
#include "function/list/functions/list_position_function.h"
#include "function/list/functions/list_unique_function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListIntersect {
    static void operation(common::list_entry_t& left, common::list_entry_t& right,
        common::list_entry_t& result, common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& resultVector) {
        int64_t pos = 0;
        auto rightDataVector = common::ListVector::getDataVector(&rightVector);
        auto rightPos = right.offset;
        std::vector<offset_t> rightOffsets;
        for (auto i=0u; i < right.size; i++) {
            common::TypeUtils::visit(ListType::getChildType(rightVector.dataType).getPhysicalType(),
            [&]<typename T>(T) {
                    if (rightDataVector->isNull(right.offset + i)) {
                        return;
                    }
                    ListPosition::operation(left,
                        *(T*)ListVector::getListValuesWithOffset(&rightVector, right, i), pos,
                        leftVector, *ListVector::getDataVector(&rightVector), resultVector);
                });
            if (pos !=0) {
                rightOffsets.push_back(rightPos+i);
            }
        }
        common::ValueVector tempVec(kuzu::common::LogicalType::LIST(rightDataVector->dataType.getLogicalTypeID()), nullptr);
        auto tempDataVec=common::ListVector::getDataVector(&tempVec);
        auto temp = common::ListVector::addList(&tempVec, rightOffsets.size());
        auto tempPos = temp.offset;
        for (auto i=0u; i<rightOffsets.size(); i++) {
            tempDataVec->copyFromVectorData(tempPos++, rightDataVector, rightPos+rightOffsets.at(i));
        }
        auto numUniqueValues = ListUnique::appendListElementsToValueSet(temp, tempVec);
        result = common::ListVector::addList(&resultVector, numUniqueValues);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto resultDataVectorBuffer =
            common::ListVector::getListValuesWithOffset(&resultVector, result, 0 /* offset */);
        ListUnique::appendListElementsToValueSet(temp, tempVec, nullptr,
            [&resultDataVector, &resultDataVectorBuffer](common::ValueVector& dataVector,
                uint64_t pos) -> void {
                resultDataVector->copyFromVectorData(resultDataVectorBuffer, &dataVector,
                    dataVector.getData() + pos * dataVector.getNumBytesPerValue());
                resultDataVectorBuffer += dataVector.getNumBytesPerValue();
            });
    }
    
};
static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());
    if (types[0] != types[1]) {
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(ListIntersectFunction::name,
            types[0].toString(), types[1].toString()));
    }
    return std::make_unique<FunctionBindData>(std::move(types), types[0].copy());
}

function_set ListIntersectFunction::getFunctionSet() {
    function_set result;
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, ListIntersect>;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::LIST,
        execFunc);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
