#include "processor/operator/index_lookup.h"

#include "common/exception/message.h"
#include "storage/index/hash_index.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

bool IndexLookup::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& info : infos) {
        KU_ASSERT(info);
        indexLookup(context->clientContext->getActiveTransaction(), *info);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> IndexLookup::clone() {
    std::vector<std::unique_ptr<IndexLookupInfo>> copiedInfos;
    copiedInfos.reserve(infos.size());
    for (const auto& info : infos) {
        copiedInfos.push_back(info->copy());
    }
    return make_unique<IndexLookup>(
        std::move(copiedInfos), children[0]->clone(), getOperatorID(), paramsString);
}

void IndexLookup::indexLookup(transaction::Transaction* transaction, const IndexLookupInfo& info) {
    auto keyVector = resultSet->getValueVector(info.keyVectorPos).get();
    checkNullKeys(keyVector);
    auto resultVector = resultSet->getValueVector(info.resultVectorPos).get();
    fillOffsetArraysFromVector(transaction, info, keyVector, resultVector);
}

// TODO(Guodong): Add short path for unfiltered case.
void IndexLookup::checkNullKeys(ValueVector* keyVector) {
    if (keyVector->hasNoNullsGuarantee()) {
        return;
    }
    for (auto i = 0u; i < keyVector->state->selVector->selectedSize; i++) {
        auto pos = keyVector->state->selVector->selectedPositions[i];
        if (keyVector->isNull(pos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
    }
}

// TODO(Guodong): Add short path for unfiltered case.
void IndexLookup::fillOffsetArraysFromVector(transaction::Transaction* transaction,
    const IndexLookupInfo& info, ValueVector* keyVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    auto offsets = (offset_t*)resultVector->getData();
    auto numKeys = keyVector->state->selVector->selectedSize;
    switch (info.pkDataType->getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        for (auto i = 0u; i < numKeys; i++) {
            auto pos = keyVector->state->selVector->selectedPositions[i];
            auto key = keyVector->getValue<int64_t>(pos);
            if (!info.pkIndex->lookup(transaction, key, offsets[i])) {
                throw RuntimeException(ExceptionMessage::nonExistPKException(std::to_string(key)));
            }
        }
    } break;
    case LogicalTypeID::STRING: {
        for (auto i = 0u; i < numKeys; i++) {
            auto key =
                keyVector->getValue<ku_string_t>(keyVector->state->selVector->selectedPositions[i]);
            if (!info.pkIndex->lookup(transaction, key.getAsString().c_str(), offsets[i])) {
                throw RuntimeException(ExceptionMessage::nonExistPKException(key.getAsString()));
            }
        }
    } break;
    case LogicalTypeID::SERIAL: {
        for (auto i = 0u; i < numKeys; i++) {
            auto pos = keyVector->state->selVector->selectedPositions[i];
            offsets[i] = keyVector->getValue<int64_t>(pos);
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace processor
} // namespace kuzu
