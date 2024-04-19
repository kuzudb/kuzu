#include "function/list/functions/list_to_string_function.h"

#include "common/type_utils.h"

namespace kuzu {
namespace function {

void ListToString::operation(common::list_entry_t& input, common::ku_string_t& delim,
    common::ku_string_t& result, common::ValueVector& inputVector,
    common::ValueVector& /*delimVector*/, common::ValueVector& resultVector) {
    std::string resultStr = "";
    auto dataVector = common::ListVector::getDataVector(&inputVector);
    for (auto i = 0u; i < input.size - 1; i++) {
        resultStr += common::TypeUtils::entryToString(dataVector->dataType,
            common::ListVector::getListValuesWithOffset(&inputVector, input, i), dataVector);
        resultStr += delim.getAsString();
    }
    resultStr += common::TypeUtils::entryToString(dataVector->dataType,
        common::ListVector::getListValuesWithOffset(&inputVector, input, input.size - 1),
        dataVector);
    common::StringVector::addString(&resultVector, result, resultStr);
}

} // namespace function
} // namespace kuzu
