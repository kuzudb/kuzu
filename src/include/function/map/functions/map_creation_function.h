#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct MapCreation {
    static void operation(common::list_entry_t& keyEntry, common::list_entry_t& valueEntry,
        common::list_entry_t& resultEntry, common::ValueVector& keyVector,
        common::ValueVector& valueVector, common::ValueVector& resultVector) {
        if (keyEntry.size != valueEntry.size) {
            throw common::RuntimeException{"Unaligned key list and value list."};
        }
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
