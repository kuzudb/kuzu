#include "function/list/functions/list_unique_function.h"

#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

uint64_t ListUnique::appendListElementsToValueSet(common::list_entry_t& input,
    common::ValueVector& inputVector, duplicate_value_handler duplicateValHandler,
    unique_value_handler uniqueValueHandler, null_value_handler nullValueHandler) {
    ValueSet uniqueKeys;
    auto dataVector = common::ListVector::getDataVector(&inputVector);
    auto val = common::Value::createDefaultValue(dataVector->dataType);
    for (auto i = 0u; i < input.size; i++) {
        if (dataVector->isNull(input.offset + i)) {
            if (nullValueHandler != nullptr) {
                nullValueHandler();
            }
            continue;
        }
        auto entryVal = common::ListVector::getListValuesWithOffset(&inputVector, input, i);
        val.copyFromColLayout(entryVal, dataVector);
        auto uniqueKey = uniqueKeys.insert(val).second;
        if (duplicateValHandler != nullptr && !uniqueKey) {
            duplicateValHandler(
                common::TypeUtils::entryToString(dataVector->dataType, entryVal, dataVector));
        }
        if (uniqueValueHandler != nullptr && uniqueKey) {
            uniqueValueHandler(*dataVector, input.offset + i);
        }
    }
    return uniqueKeys.size();
}

void ListUnique::operation(common::list_entry_t& input, int64_t& result,
    common::ValueVector& inputVector, common::ValueVector& /*resultVector*/) {
    result = appendListElementsToValueSet(input, inputVector);
}

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function*) {
    return FunctionBindData::getSimpleBindData(arguments, LogicalType::INT64());
}

function_set ListUniqueFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, ListUnique>, nullptr,
        bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
