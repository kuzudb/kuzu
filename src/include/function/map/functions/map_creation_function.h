#pragma once

#include "unordered_set"

#include "common/exception/runtime.h"
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

static void validateKeys(common::list_entry_t& keyEntry, common::ValueVector& keyVector) {
    std::unordered_set<common::Value, ValueHashFunction, ValueEquality> uniqueKeys;
    auto dataVector = common::ListVector::getDataVector(&keyVector);
    auto val = common::Value::createDefaultValue(dataVector->dataType);
    for (auto i = 0u; i < keyEntry.size; i++) {
        auto entryVal = common::ListVector::getListValuesWithOffset(&keyVector, keyEntry, i);
        val.copyFromColLayout(entryVal, dataVector);
        auto unique = uniqueKeys.insert(val).second;
        if (!unique) {
            throw common::RuntimeException{common::stringFormat("Found duplicate key: {} in map.",
                common::TypeUtils::entryToString(dataVector->dataType, entryVal, dataVector))};
        }
    }
}

struct MapCreation {
    static void operation(common::list_entry_t& keyEntry, common::list_entry_t& valueEntry,
        common::list_entry_t& resultEntry, common::ValueVector& keyVector,
        common::ValueVector& valueVector, common::ValueVector& resultVector) {
        if (keyEntry.size != valueEntry.size) {
            throw common::RuntimeException{"Unaligned key list and value list."};
        }
        validateKeys(keyEntry, keyVector);
        resultEntry = common::ListVector::addList(&resultVector, keyEntry.size);
        auto resultStructVector = common::ListVector::getDataVector(&resultVector);
        copyListEntry(resultEntry,
            common::StructVector::getFieldVector(resultStructVector, 0 /* keyVector */).get(),
            keyEntry, &keyVector);
        copyListEntry(resultEntry,
            common::StructVector::getFieldVector(resultStructVector, 1 /* valueVector */).get(),
            valueEntry, &valueVector);
    }

    static void copyListEntry(common::list_entry_t& resultEntry, common::ValueVector* resultVector,
        common::list_entry_t& srcEntry, common::ValueVector* srcVector) {
        auto resultPos = resultEntry.offset;
        auto srcDataVector = common::ListVector::getDataVector(srcVector);
        auto srcPos = srcEntry.offset;
        for (auto i = 0u; i < srcEntry.size; i++) {
            resultVector->copyFromVectorData(resultPos++, srcDataVector, srcPos++);
        }
    }
};

} // namespace function
} // namespace kuzu
