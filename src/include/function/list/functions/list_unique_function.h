#pragma once

#include "common/type_utils.h"
#include "common/types/value/value.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ValueHashFunction {
    uint64_t operator()(const common::Value& value) const { return (uint64_t)value.computeHash(); }
};

struct ValueEquality {
    bool operator()(const common::Value& a, const common::Value& b) const { return a == b; }
};

using ValueSet = std::unordered_set<common::Value, ValueHashFunction, ValueEquality>;

using duplicate_value_handler = std::function<void(const std::string&)>;
using unique_value_handler = std::function<void(common::ValueVector& dataVector, uint64_t pos)>;
using null_value_handler = std::function<void()>;

struct ListUnique {
    static uint64_t appendListElementsToValueSet(common::list_entry_t& input,
        common::ValueVector& inputVector, duplicate_value_handler duplicateValHandler = nullptr,
        unique_value_handler uniqueValueHandler = nullptr,
        null_value_handler nullValueHandler = nullptr) {
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

    static void operation(common::list_entry_t& input, int64_t& result,
        common::ValueVector& inputVector, common::ValueVector& /*resultVector*/) {
        result = appendListElementsToValueSet(input, inputVector);
    }
};

} // namespace function
} // namespace kuzu
