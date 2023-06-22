#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

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
        auto resultValues =
            resultVector->getData() + resultVector->getNumBytesPerValue() * resultEntry.offset;
        auto srcValues = common::ListVector::getListValues(srcVector, srcEntry);
        auto srcDataVector = common::ListVector::getDataVector(srcVector);
        for (auto i = 0u; i < srcEntry.size; i++) {
            resultVector->copyFromVectorData(resultValues, srcDataVector, srcValues);
            srcValues += srcDataVector->getNumBytesPerValue();
            resultValues += resultVector->getNumBytesPerValue();
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
