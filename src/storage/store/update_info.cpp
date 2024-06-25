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
    auto& vectorUpdateInfo =
        getOrCreateVectorInfo(transaction, vectorIdx, rowIdxInVector, values.dataType);
    // Check if the row is already updated in this transaction. Overwrite if so.
    idx_t idxInUpdateData = INVALID_IDX;
    for (auto i = 0u; i < vectorUpdateInfo.numRowsUpdated; i++) {
        if (vectorUpdateInfo.rowsInVector[i] == rowIdxInVector) {
            idxInUpdateData = i;
            break;
        }
    }
    if (idxInUpdateData != INVALID_IDX) {
        // Overwrite existing update value.
        vectorUpdateInfo.data->write(&values, values.state->getSelVector()[0], idxInUpdateData);
    } else {
        // Append new value and update `rowsInVector`.
        vectorUpdateInfo.rowsInVector[vectorUpdateInfo.numRowsUpdated] = rowIdxInVector;
        vectorUpdateInfo.data->write(&values, values.state->getSelVector()[0],
            vectorUpdateInfo.numRowsUpdated++);
    }
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

VectorUpdateInfo& UpdateInfo::getOrCreateVectorInfo(const Transaction* transaction, idx_t idx,
    sel_t rowIdxInVector, const LogicalType& dataType) {
    if (idx >= vectorsInfo.size()) {
        vectorsInfo.resize(idx + 1);
    }
    if (!vectorsInfo[idx]) {
        vectorsInfo[idx] =
            std::make_unique<VectorUpdateInfo>(transaction->getID(), dataType.copy());
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
        auto newInfo = std::make_unique<VectorUpdateInfo>(transaction->getID(), dataType.copy());
        vectorsInfo[idx]->next = newInfo.get();
        newInfo->prev = std::move(vectorsInfo[idx]);
        vectorsInfo[idx] = std::move(newInfo);
        info = vectorsInfo[idx].get();
        if (info->prev) {
            // Copy the data from the previous version.
            for (auto i = 0u; i < info->prev->numRowsUpdated; i++) {
                info->rowsInVector[i] = info->prev->rowsInVector[i];
            }
            info->data->append(info->prev->data.get(), 0, info->prev->numRowsUpdated);
        }
    }
    return *info;
}

} // namespace storage
} // namespace kuzu
