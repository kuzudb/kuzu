#pragma once

#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "common/types/value/value.h"
#include "common/vector/value_vector.h"
#include "function/list/functions/list_unique_function.h"
#include "main/client_context.h"

namespace kuzu {
namespace function {

static void duplicateValueHandler(const std::string& key, main::ClientContext* context) {
    if (context->getClientConfig()->allowMapDuplicateKey) {
        return;
    }
    throw common::RuntimeException{common::stringFormat("Found duplicate key: {} in map.", key)};
}

static void nullValueHandler() {
    throw common::RuntimeException("Null value key is not allowed in map.");
}

static void validateKeys(common::list_entry_t& keyEntry, common::ValueVector& keyVector,
    FunctionBindData* bindData) {
    ListUnique::appendListElementsToValueSet(keyEntry, keyVector,
        std::bind(duplicateValueHandler, std::placeholders::_1, bindData->clientContext),
        nullptr /* uniqueValueHandler */, nullValueHandler);
}

struct MapCreation {
    static void operation(common::list_entry_t& keyEntry, common::list_entry_t& valueEntry,
        common::list_entry_t& resultEntry, common::ValueVector& keyVector,
        common::ValueVector& valueVector, common::ValueVector& resultVector, void* dataPtr) {
        if (keyEntry.size != valueEntry.size) {
            throw common::RuntimeException{"Unaligned key list and value list."};
        }

        validateKeys(keyEntry, keyVector, reinterpret_cast<FunctionBindData*>(dataPtr));
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
