#pragma once

#include "common/type_utils.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct ListToString {
public:
    static void operation(common::list_entry_t& input, common::ku_string_t& delim,
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
};

} // namespace function
} // namespace kuzu
