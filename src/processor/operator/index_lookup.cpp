#include "processor/operator/index_lookup.h"

#include "common/assert.h"
#include "common/exception/message.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/index/hash_index.h"
#include "transaction/transaction.h"

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
        lookup(context->clientContext->getTx(), *info);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> IndexLookup::clone() {
    std::vector<std::unique_ptr<IndexLookupInfo>> copiedInfos;
    copiedInfos.reserve(infos.size());
    for (const auto& info : infos) {
        copiedInfos.push_back(info->copy());
    }
    return make_unique<IndexLookup>(std::move(copiedInfos), children[0]->clone(), getOperatorID(),
        paramsString);
}

void IndexLookup::setBatchInsertSharedState(std::shared_ptr<BatchInsertSharedState> sharedState) {
    for (auto& info : infos) {
        KU_ASSERT(info->batchInsertSharedState == nullptr);
        info->batchInsertSharedState = sharedState;
    }
}

void IndexLookup::lookup(transaction::Transaction* transaction, const IndexLookupInfo& info) {
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
    for (auto i = 0u; i < keyVector->state->getSelVector().getSelSize(); i++) {
        auto pos = keyVector->state->getSelVector()[i];
        if (keyVector->isNull(pos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
    }
}

void stringPKFillOffsetArraysFromVector(transaction::Transaction* transaction,
    const IndexLookupInfo& info, ValueVector* keyVector, offset_t* offsets) {
    auto numKeys = keyVector->state->getSelVector().getSelSize();
    for (auto i = 0u; i < numKeys; i++) {
        auto key = keyVector->getValue<ku_string_t>(keyVector->state->getSelVector()[i]);
        if (!info.index->lookup(transaction, key.getAsStringView(), offsets[i])) {
            throw RuntimeException(ExceptionMessage::nonExistentPKException(key.getAsString()));
        }
    }
}

template<HashablePrimitive T>
void primitivePKFillOffsetArraysFromVector(transaction::Transaction* transaction,
    const IndexLookupInfo& info, ValueVector* keyVector, offset_t* offsets) {
    auto numKeys = keyVector->state->getSelVector().getSelSize();
    for (auto i = 0u; i < numKeys; i++) {
        auto pos = keyVector->state->getSelVector()[i];
        auto key = keyVector->getValue<T>(pos);
        if (!info.index->lookup(transaction, key, offsets[i])) {
            throw RuntimeException(
                ExceptionMessage::nonExistentPKException(TypeUtils::toString(key)));
        }
    }
}

// TODO(Guodong): Add short path for unfiltered case.
void IndexLookup::fillOffsetArraysFromVector(transaction::Transaction* transaction,
    const IndexLookupInfo& info, ValueVector* keyVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    auto offsets = (offset_t*)resultVector->getData();
    TypeUtils::visit(
        info.pkDataType->getPhysicalType(),
        [&](ku_string_t) {
            stringPKFillOffsetArraysFromVector(transaction, info, keyVector, offsets);
        },
        [&]<HashablePrimitive T>(T) {
            if (info.pkDataType->getLogicalTypeID() == LogicalTypeID::SERIAL) {
                auto numKeys = keyVector->state->getSelVector().getSelSize();
                for (auto i = 0u; i < numKeys; i++) {
                    auto pos = keyVector->state->getSelVector()[i];
                    offsets[i] = keyVector->getValue<int64_t>(pos);
                }
            } else {
                primitivePKFillOffsetArraysFromVector<T>(transaction, info, keyVector, offsets);
            }
        },
        [&](auto) { KU_UNREACHABLE; });
}

} // namespace processor
} // namespace kuzu
