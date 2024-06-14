#include "storage/store/update_info.h"

#include "common/exception/runtime.h"
#include "storage/store/column_chunk_data.h"
#include "transaction/transaction.h"

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

VectorUpdateInfo* UpdateInfo::update(const Transaction* transaction, const idx_t vectorIdx,
    const sel_t rowIdxInVector, const ValueVector& values) {
    auto& vectorUpdateInfo = getVectorInfo(transaction, vectorIdx, rowIdxInVector);
    vectorUpdateInfo.data->write(&values, values.state->getSelVector()[0],
        vectorUpdateInfo.numRowsUpdated++);
    return &vectorUpdateInfo;
}

VectorUpdateInfo* UpdateInfo::getVectorInfo(const Transaction* transaction, idx_t idx) const {
    if (idx >= vectorsInfo.size() || !vectorsInfo[idx]) {
        return nullptr;
    }
    auto current = vectorsInfo[idx].get();
    while (current) {
        if (current->version == transaction->getID()) {
            KU_ASSERT(current->version >= Transaction::START_TRANSACTION_ID);
            return current;
        }
        if (current->version <= transaction->getStartTS()) {
            KU_ASSERT(current->version < Transaction::START_TRANSACTION_ID);
            return current;
        }
        current = current->getPrev();
    }
    return nullptr;
}

VectorUpdateInfo& UpdateInfo::getVectorInfo(const Transaction* transaction, idx_t idx,
    sel_t rowIdxInVector) {
    if (idx >= vectorsInfo.size()) {
        vectorsInfo.resize(idx + 1);
    }
    if (!vectorsInfo[idx]) {
        vectorsInfo[idx] = std::make_unique<VectorUpdateInfo>(transaction->getID());
        return *vectorsInfo[idx];
    }
    auto* current = vectorsInfo[idx].get();
    VectorUpdateInfo* info = nullptr;
    while (current) {
        if (current->version == transaction->getID()) {
            // Same transaction.
            KU_ASSERT(current->version >= Transaction::START_TRANSACTION_ID);
            info = current;
        } else if (current->version >= transaction->getStartTS()) {
            // Potentially there can be conflicts. `current` can be uncommitted transaction (version
            // is transaction ID) or committed transaction started after this transaction.
            for (auto i = 0u; i < current->numRowsUpdated; i++) {
                if (current->rowsInVector[i] == rowIdxInVector) {
                    throw RuntimeException("Write-write conflict of updating the same row.");
                }
            }
        }
        current = current->next;
    }
    if (!info) {
        // Create a new version here.
        auto newInfo = std::make_unique<VectorUpdateInfo>(transaction->getID());
        vectorsInfo[idx]->next = newInfo.get();
        newInfo->prev = std::move(vectorsInfo[idx]);
        vectorsInfo[idx] = std::move(newInfo);
        info = vectorsInfo[idx].get();
    }
    return *info;
}

} // namespace storage
} // namespace kuzu
