#include "processor/operator/index_lookup.h"

#include "binder/expression/expression_util.h"
#include "common/assert.h"
#include "common/exception/message.h"
#include "common/types/types.h"
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
        KU_ASSERT(warningDataVectors.size() == CopyConstants::WARNING_METADATA_NUM_COLUMNS);
        ret.emplace(WarningSourceData{
            warningDataVectors[0]->getValue<decltype(WarningSourceData::startByteOffset)>(pos),
            warningDataVectors[1]->getValue<decltype(WarningSourceData::endByteOffset)>(pos),
            warningDataVectors[2]->getValue<decltype(WarningSourceData::fileIdx)>(pos),
            warningDataVectors[3]->getValue<decltype(WarningSourceData::blockIdx)>(pos),
            warningDataVectors[4]->getValue<decltype(WarningSourceData::rowOffsetInBlock)>(pos),
        });
    }
    return ret;
}

// TODO(Guodong): Add short path for unfiltered case.
bool checkNullKey(ValueVector* keyVector, offset_t vectorOffset,
    BatchInsertErrorHandler* errorHandler, const std::vector<ValueVector*>& warningDataVectors) {
    bool isNull = keyVector->isNull(vectorOffset);
    if (isNull) {
        errorHandler->handleError(BatchInsertCachedError{ExceptionMessage::nullPKException(),
            getWarningSourceData(warningDataVectors, vectorOffset)});
    }
    return !isNull;
}

// TODO(Guodong): Add short path for unfiltered case.
template<bool hasNoNullsGuarantee>
void fillOffsetArraysFromVector(transaction::Transaction* transaction, const IndexLookupInfo& info,
    ValueVector* keyVector, ValueVector* resultVector,
    const std::vector<ValueVector*>& warningDataVectors, BatchInsertErrorHandler* errorHandler) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    auto offsets = (offset_t*)resultVector->getData();
    TypeUtils::visit(
        keyVector->dataType.getPhysicalType(),
        [&]<IndexHashable T>(T) {
            auto numKeys = keyVector->state->getSelVector().getSelSize();
            std::vector<sel_t> lookupPos;
            lookupPos.reserve(numKeys);
            for (idx_t i = 0; i < numKeys; ++i) {
                lookupPos.push_back(keyVector->state->getSelVector()[i]);
            }

            // if we are ignoring errors we may need to filter the output sel vector
            if (errorHandler->getIgnoreErrors()) {
                resultVector->state->getSelVectorUnsafe().setToFiltered();
            }

            offset_t insertOffset = 0;
            for (auto i = 0u; i < numKeys; i++) {
                auto pos = lookupPos[i];
                if constexpr (!hasNoNullsGuarantee) {
                    if (!checkNullKey(keyVector, pos, errorHandler, warningDataVectors)) {
                        continue;
                    }
                }
                if (!info.nodeTable->lookupPK(transaction, keyVector, pos, offsets[insertOffset])) {
                    auto key = keyVector->getValue<T>(pos);
                    errorHandler->handleError(BatchInsertCachedError{
                        ExceptionMessage::nonExistentPKException(TypeUtils::toString(key)),
                        getWarningSourceData(warningDataVectors, pos)});
                } else {
                    if (errorHandler->getIgnoreErrors()) {
                        resultVector->state->getSelVectorUnsafe()[insertOffset] = i;
                    }
                    ++insertOffset;
                }
            }

            if (errorHandler->getIgnoreErrors()) {
                resultVector->state->getSelVectorUnsafe().setSelSize(insertOffset);
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
        context, ignoreErrors, sharedState->errorCounter, &sharedState->mtx));
}

std::unique_ptr<PhysicalOperator> IndexLookup::clone() {
    std::vector<std::unique_ptr<IndexLookupInfo>> copiedInfos;
    copiedInfos.reserve(infos.size());
    for (const auto& info : infos) {
        copiedInfos.push_back(info->copy());
    }
    return make_unique<IndexLookup>(std::move(copiedInfos), ignoreErrors, sharedState,
        children[0]->clone(), getOperatorID(), printInfo->copy());
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

    std::vector<ValueVector*> warningDataVectors;
    warningDataVectors.reserve(info.warningDataPos.size());
    for (size_t i = 0; i < info.warningDataPos.size(); ++i) {
        warningDataVectors.push_back(resultSet->getValueVector(info.warningDataPos[i]).get());
    }

    if (keyVector->hasNoNullsGuarantee()) {
        fillOffsetArraysFromVector<true>(transaction, info, keyVector, resultVector,
            warningDataVectors, localState->errorHandler.get());
    } else {
        fillOffsetArraysFromVector<false>(transaction, info, keyVector, resultVector,
            warningDataVectors, localState->errorHandler.get());
    }
}

} // namespace processor
} // namespace kuzu
