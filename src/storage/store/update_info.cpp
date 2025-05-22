#include "storage/table/update_info.h"

#include <algorithm>

#include "common/exception/runtime.h"
#include "common/vector/value_vector.h"
#include "storage/storage_utils.h"
#include "storage/table/column_chunk_data.h"
#include "transaction/transaction.h"

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

VectorUpdateInfo* UpdateInfo::update(MemoryManager& memoryManager, const Transaction* transaction,
    const idx_t vectorIdx, const sel_t rowIdxInVector, const ValueVector& values) {
    auto& vectorUpdateInfo = getOrCreateVectorInfo(memoryManager, transaction, vectorIdx,
        rowIdxInVector, values.dataType);
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

row_idx_t UpdateInfo::getNumUpdatedRows(const Transaction* transaction) const {
    row_idx_t numUpdatedRows = 0u;
    for (auto i = 0u; i < vectorsInfo.size(); i++) {
        if (const auto vectorInfo = getVectorInfo(transaction, i)) {
            numUpdatedRows += vectorInfo->numRowsUpdated;
        }
    }
    return numUpdatedRows;
}

bool UpdateInfo::hasUpdates(const Transaction* transaction, row_idx_t startRow,
    length_t numRows) const {
    auto [startVector, rowInStartVector] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, rowInEndVector] =
        StorageUtils::getQuotientRemainder(startRow + numRows, DEFAULT_VECTOR_CAPACITY);
    for (idx_t vectorIdx = startVector; vectorIdx <= endVectorIdx; ++vectorIdx) {
        const auto updateVector = getVectorInfo(transaction, vectorIdx);
        if (!updateVector || updateVector->numRowsUpdated == 0) {
            continue;
        }
        const auto startRowInVector = (vectorIdx == startVector) ? rowInStartVector : 0;
        const auto endRowInVector =
            (vectorIdx == endVectorIdx) ? rowInEndVector : DEFAULT_VECTOR_CAPACITY;
        for (auto row = startRowInVector; row <= endRowInVector; row++) {
            if (std::any_of(updateVector->rowsInVector.begin(),
                    updateVector->rowsInVector.begin() + updateVector->numRowsUpdated,
                    [&](row_idx_t updatedRow) { return row == updatedRow; })) {
                return true;
            }
        }
    }
    return false;
}

VectorUpdateInfo& UpdateInfo::getOrCreateVectorInfo(MemoryManager& memoryManager,
    const Transaction* transaction, idx_t vectorIdx, sel_t rowIdxInVector,
    const LogicalType& dataType) {
    if (vectorIdx >= vectorsInfo.size()) {
        vectorsInfo.resize(vectorIdx + 1);
    }
    if (!vectorsInfo[vectorIdx]) {
        vectorsInfo[vectorIdx] = std::make_unique<VectorUpdateInfo>(memoryManager,
            transaction->getID(), dataType.copy());
        return *vectorsInfo[vectorIdx];
    }
    auto* current = vectorsInfo[vectorIdx].get();
    VectorUpdateInfo* info = nullptr;
    while (current) {
        if (current->version == transaction->getID()) {
            // Same transaction.
            KU_ASSERT(current->version >= Transaction::START_TRANSACTION_ID);
            info = current;
        } else if (current->version > transaction->getStartTS()) {
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
        auto newInfo = std::make_unique<VectorUpdateInfo>(memoryManager, transaction->getID(),
            dataType.copy());
        vectorsInfo[vectorIdx]->next = newInfo.get();
        newInfo->prev = std::move(vectorsInfo[vectorIdx]);
        vectorsInfo[vectorIdx] = std::move(newInfo);
        info = vectorsInfo[vectorIdx].get();
        if (info->prev) {
            // Copy the data from the previous version.
            for (auto i = 0u; i < info->prev->numRowsUpdated; i++) {
                info->rowsInVector[i] = info->prev->rowsInVector[i];
            }
            info->data->append(info->prev->data.get(), 0, info->prev->numRowsUpdated);
            info->numRowsUpdated = info->prev->numRowsUpdated;
        }
    }
    return *info;
}

} // namespace storage
} // namespace kuzu
