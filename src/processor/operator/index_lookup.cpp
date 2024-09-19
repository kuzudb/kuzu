#include "processor/operator/index_lookup.h"

#include "binder/expression/expression_util.h"
#include "common/assert.h"
#include "common/exception/message.h"
#include "common/types/types.h"
#include "common/utils.h"
#include "common/vector/value_vector.h"
#include "storage/index/hash_index.h"
#include "storage/store/node_table.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

namespace {

std::optional<WarningSourceData> getWarningSourceData(
    const std::vector<ValueVector*>& warningDataVectors, sel_t pos) {
    std::optional<WarningSourceData> ret;
    if (!warningDataVectors.empty()) {
        ret.emplace(WarningSourceData::constructFromData(warningDataVectors,
            safeIntegerConversion<idx_t>(pos)));
    }
    return ret;
}

// TODO(Guodong): Add short path for unfiltered case.
bool checkNullKey(ValueVector* keyVector, offset_t vectorOffset,
    BatchInsertErrorHandler* errorHandler, const std::vector<ValueVector*>& warningDataVectors) {
    bool isNull = keyVector->isNull(vectorOffset);
    if (isNull) {
        errorHandler->handleError(ExceptionMessage::nullPKException(),
            getWarningSourceData(warningDataVectors, vectorOffset));
    }
    return !isNull;
}

struct OffsetVectorManager {
    OffsetVectorManager(ValueVector* resultVector, BatchInsertErrorHandler* errorHandler)
        : ignoreErrors(errorHandler->getIgnoreErrors()), resultVector(resultVector),
          insertOffset(0) {
        // if we are ignoring errors we may need to filter the output sel vector
        if (ignoreErrors) {
            resultVector->state->getSelVectorUnsafe().setToFiltered();
        }
    }

    ~OffsetVectorManager() {
        if (ignoreErrors) {
            resultVector->state->getSelVectorUnsafe().setSelSize(insertOffset);
        }
    }

    void insertEntry(offset_t entry, sel_t posInKeyVector) {
        auto* offsets = reinterpret_cast<offset_t*>(resultVector->getData());
        offsets[posInKeyVector] = entry;
        if (ignoreErrors) {
            // if the lookup was successful we may add the current entry to the output selection
            resultVector->state->getSelVectorUnsafe()[insertOffset] = posInKeyVector;
        }
        ++insertOffset;
    }

    bool ignoreErrors;
    ValueVector* resultVector;

    offset_t insertOffset;
};

// TODO(Guodong): Add short path for unfiltered case.
template<bool hasNoNullsGuarantee>
void fillOffsetArraysFromVector(transaction::Transaction* transaction, const IndexLookupInfo& info,
    ValueVector* keyVector, ValueVector* resultVector,
    const std::vector<ValueVector*>& warningDataVectors, BatchInsertErrorHandler* errorHandler) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    TypeUtils::visit(
        keyVector->dataType.getPhysicalType(),
        [&]<IndexHashable T>(T) {
            auto numKeys = keyVector->state->getSelVector().getSelSize();

            // fetch all the selection pos at the start
            // since we may modify the selection vector in the middle of the lookup
            std::vector<sel_t> lookupPos(numKeys);
            for (idx_t i = 0; i < numKeys; ++i) {
                lookupPos[i] = (keyVector->state->getSelVector()[i]);
            }

            OffsetVectorManager resultManager{resultVector, errorHandler};
            for (auto i = 0u; i < numKeys; i++) {
                auto pos = lookupPos[i];
                if constexpr (!hasNoNullsGuarantee) {
                    if (!checkNullKey(keyVector, pos, errorHandler, warningDataVectors)) {
                        continue;
                    }
                }
                offset_t lookupOffset = 0;
                if (!info.nodeTable->lookupPK(transaction, keyVector, pos, lookupOffset)) {
                    auto key = keyVector->getValue<T>(pos);
                    errorHandler->handleError(
                        ExceptionMessage::nonExistentPKException(TypeUtils::toString(key)),
                        getWarningSourceData(warningDataVectors, pos));
                } else {
                    resultManager.insertEntry(lookupOffset, pos);
                }
            }
        },
        [&](auto) { KU_UNREACHABLE; });
}
} // namespace

std::string IndexLookupPrintInfo::toString() const {
    std::string result = "Indexes: ";
    result += binder::ExpressionUtil::toString(expressions);
    return result;
}

bool IndexLookup::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& info : infos) {
        KU_ASSERT(info);
        lookup(context->clientContext->getTx(), *info);
    }
    localState->errorHandler->flushStoredErrors();
    return true;
}

void IndexLookup::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    localState = std::make_unique<IndexLookupLocalState>(std::make_unique<BatchInsertErrorHandler>(
        context, context->clientContext->getWarningContext().getIgnoreErrorsOption()));
    KU_ASSERT(!infos.empty());
    const auto& info = infos[0];
    localState->warningDataVectors.reserve(info->warningDataVectorPos.size());
    for (size_t i = 0; i < info->warningDataVectorPos.size(); ++i) {
        localState->warningDataVectors.push_back(
            resultSet->getValueVector(info->warningDataVectorPos[i]).get());
    }
    RUNTIME_CHECK(for (idx_t i = 1; i < infos.size(); ++i) {
        KU_ASSERT(infos[i]->warningDataVectorPos.size() == infos[0]->warningDataVectorPos.size());
        for (idx_t j = 0; j < infos[i]->warningDataVectorPos.size(); ++j) {
            KU_ASSERT(infos[i]->warningDataVectorPos[j] == infos[0]->warningDataVectorPos[j]);
        }
    });
}

std::unique_ptr<PhysicalOperator> IndexLookup::clone() {
    std::vector<std::unique_ptr<IndexLookupInfo>> copiedInfos;
    copiedInfos.reserve(infos.size());
    for (const auto& info : infos) {
        copiedInfos.push_back(info->copy());
    }
    return make_unique<IndexLookup>(std::move(copiedInfos), children[0]->clone(), getOperatorID(),
        printInfo->copy());
}

void IndexLookup::setBatchInsertSharedState(std::shared_ptr<BatchInsertSharedState> sharedState) {
    for (auto& info : infos) {
        KU_ASSERT(info->batchInsertSharedState == nullptr);
        info->batchInsertSharedState = sharedState;
    }
}

void IndexLookup::lookup(transaction::Transaction* transaction, const IndexLookupInfo& info) {
    auto keyVector = resultSet->getValueVector(info.keyVectorPos).get();
    auto resultVector = resultSet->getValueVector(info.resultVectorPos).get();

    if (keyVector->hasNoNullsGuarantee()) {
        fillOffsetArraysFromVector<true>(transaction, info, keyVector, resultVector,
            localState->warningDataVectors, localState->errorHandler.get());
    } else {
        fillOffsetArraysFromVector<false>(transaction, info, keyVector, resultVector,
            localState->warningDataVectors, localState->errorHandler.get());
    }
}

} // namespace processor
} // namespace kuzu
