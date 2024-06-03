#include "storage/store/version_info.h"

#include "common/exception/runtime.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

row_idx_t VectorVersionInfo::append(transaction_t transactionID, row_idx_t startRow,
    row_idx_t numRows) {
    anyInserted = true;
    for (auto i = 0u; i < numRows; i++) {
        KU_ASSERT(insertedVersions[startRow + i] == INVALID_TRANSACTION);
        insertedVersions[startRow + i] = transactionID;
    }
    return true;
}

bool VectorVersionInfo::delete_(transaction_t transactionID, row_idx_t rowIdx) {
    anyDeleted = true;
    if (deletedVersions[rowIdx] == transactionID) {
        return false;
    }
    if (deletedVersions[rowIdx] != INVALID_TRANSACTION) {
        throw RuntimeException(
            "Write-write conflict: deleting a row that is already deleted by another transaction.");
    }
    deletedVersions[rowIdx] = transactionID;
    return true;
}

void VectorVersionInfo::getSelVectorForScan(transaction_t startTS, transaction_t transactionID,
    SelectionVector& selVector, row_idx_t startRow, row_idx_t numRows) const {
    const auto numValues = selVector.getSelSize();
    auto numSelected = selVector.getSelSize();
    if (!anyDeleted && !anyInserted) {
        for (auto i = 0u; i < numRows; i++) {
            selVector.getMultableBuffer()[numSelected++] = numValues + i;
        }
    } else {
        for (auto i = 0u; i < numRows; i++) {
            const auto rowIdx = startRow + i;
            if (isInserted(startTS, transactionID, rowIdx) &&
                !isDeleted(startTS, transactionID, rowIdx)) {
                selVector.getMultableBuffer()[numSelected++] = numValues + i;
            }
        }
    }
    selVector.setToFiltered(numSelected);
}

bool VectorVersionInfo::isDeleted(transaction_t startTS, transaction_t transactionID,
    row_idx_t rowIdx) const {
    const auto deletion = deletedVersions[rowIdx];
    const auto isDeletedWithinSameTransaction = deletion == transactionID;
    const auto isDeletedByPrevCommittedTransaction = deletion < startTS;
    return isDeletedWithinSameTransaction || isDeletedByPrevCommittedTransaction;
}

bool VectorVersionInfo::isInserted(transaction_t startTS, transaction_t transactionID,
    row_idx_t rowIdx) const {
    const auto insertion = insertedVersions[rowIdx];
    const auto isInsertedWithinSameTransaction = insertion == transactionID;
    const auto isInsertedByPrevCommittedTransaction = insertion < startTS;
    return isInsertedWithinSameTransaction || isInsertedByPrevCommittedTransaction;
}

VectorVersionInfo& NodeGroupVersionInfo::getVersionInfo(vector_idx_t vectorIdx) {
    if (vectorsInfo.size() <= vectorIdx) {
        vectorsInfo.resize(vectorIdx + 1);
    }
    if (!vectorsInfo[vectorIdx]) {
        vectorsInfo[vectorIdx] = std::make_unique<VectorVersionInfo>();
    }
    return *vectorsInfo[vectorIdx];
}

const VectorVersionInfo& NodeGroupVersionInfo::getVersionInfo(vector_idx_t vectorIdx) const {
    KU_ASSERT(vectorIdx < vectorsInfo.size());
    KU_ASSERT(vectorsInfo[vectorIdx]);
    return *vectorsInfo[vectorIdx];
}

row_idx_t NodeGroupVersionInfo::append(transaction_t transactionID, row_idx_t startRow,
    row_idx_t numRows) {
    auto [startVectorIdx, startRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow + numRows, DEFAULT_VECTOR_CAPACITY);
    row_idx_t numAppended = 0u;
    for (auto vectorIdx = startVectorIdx; vectorIdx <= endVectorIdx; vectorIdx++) {
        auto& vectorVersionInfo = getVersionInfo(vectorIdx);
        auto startRowIdx = vectorIdx == startVectorIdx ? startRowIdxInVector : 0;
        auto endRowIdx = vectorIdx == endVectorIdx ? endRowIdxInVector : DEFAULT_VECTOR_CAPACITY;
        numAppended +=
            vectorVersionInfo.append(transactionID, startRowIdx, endRowIdx - startRowIdx);
    }
    return numAppended;
}

bool NodeGroupVersionInfo::delete_(transaction_t transactionID, row_idx_t rowIdx) {
    auto [vectorIdx, rowIdxInVector] =
        StorageUtils::getQuotientRemainder(rowIdx, DEFAULT_VECTOR_CAPACITY);
    auto& vectorVersionInfo = getVersionInfo(vectorIdx);
    return vectorVersionInfo.delete_(transactionID, rowIdxInVector);
}

void NodeGroupVersionInfo::getSelVectorToScan(transaction_t startTS, transaction_t transactionID,
    SelectionVector& selVector, row_idx_t startRow, row_idx_t numRows) const {
    auto [startVectorIdx, startRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow + numRows, DEFAULT_VECTOR_CAPACITY);
    auto vectorIdx = startVectorIdx;
    while (vectorIdx <= endVectorIdx) {
        auto& vectorVersionInfo = getVersionInfo(startVectorIdx);
        const auto startRowIdx = vectorIdx == startVectorIdx ? startRowIdxInVector : 0;
        const auto endRowIdx =
            vectorIdx == endVectorIdx ? endRowIdxInVector : DEFAULT_VECTOR_CAPACITY;
        vectorVersionInfo.getSelVectorForScan(startTS, transactionID, selVector, startRowIdx,
            endRowIdx - startRowIdx);
        vectorIdx++;
    }
}

} // namespace storage
} // namespace kuzu
