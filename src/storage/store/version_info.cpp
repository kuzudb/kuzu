#include "storage/store/version_info.h"

#include "common/exception/runtime.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

row_idx_t VectorVersionInfo::append(const transaction_t transactionID, const row_idx_t startRow,
    const row_idx_t numRows) {
    anyInserted = true;
    for (auto i = 0u; i < numRows; i++) {
        KU_ASSERT(insertedVersions[startRow + i] == INVALID_TRANSACTION);
        insertedVersions[startRow + i] = transactionID;
    }
    return true;
}

bool VectorVersionInfo::delete_(const transaction_t transactionID, const row_idx_t rowIdx) {
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

void VectorVersionInfo::getSelVectorForScan(const transaction_t startTS,
    const transaction_t transactionID, SelectionVector& selVector, const row_idx_t startRow,
    const row_idx_t numRows) const {
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

bool VectorVersionInfo::isDeleted(const transaction_t startTS, const transaction_t transactionID,
    const row_idx_t rowIdx) const {
    const auto deletion = deletedVersions[rowIdx];
    const auto isDeletedWithinSameTransaction = deletion == transactionID;
    const auto isDeletedByPrevCommittedTransaction = deletion <= startTS;
    return isDeletedWithinSameTransaction || isDeletedByPrevCommittedTransaction;
}

bool VectorVersionInfo::isInserted(const transaction_t startTS, const transaction_t transactionID,
    const row_idx_t rowIdx) const {
    const auto insertion = insertedVersions[rowIdx];
    const auto isInsertedWithinSameTransaction = insertion == transactionID;
    const auto isInsertedByPrevCommittedTransaction = insertion <= startTS;
    return isInsertedWithinSameTransaction || isInsertedByPrevCommittedTransaction;
}

VectorVersionInfo& NodeGroupVersionInfo::getVersionInfo(idx_t vectorIdx) {
    if (vectorsInfo.size() <= vectorIdx) {
        vectorsInfo.resize(vectorIdx + 1);
    }
    if (!vectorsInfo[vectorIdx]) {
        vectorsInfo[vectorIdx] = std::make_unique<VectorVersionInfo>();
    }
    return *vectorsInfo[vectorIdx];
}

const VectorVersionInfo& NodeGroupVersionInfo::getVersionInfo(idx_t vectorIdx) const {
    KU_ASSERT(vectorIdx < vectorsInfo.size());
    KU_ASSERT(vectorsInfo[vectorIdx]);
    return *vectorsInfo[vectorIdx];
}

row_idx_t NodeGroupVersionInfo::append(const transaction::Transaction* transaction,
    const row_idx_t startRow, const row_idx_t numRows) {
    auto [startVectorIdx, startRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow + numRows, DEFAULT_VECTOR_CAPACITY);
    row_idx_t numAppended = 0u;
    for (auto vectorIdx = startVectorIdx; vectorIdx <= endVectorIdx; vectorIdx++) {
        auto& vectorVersionInfo = getVersionInfo(vectorIdx);
        const auto startRowIdx = vectorIdx == startVectorIdx ? startRowIdxInVector : 0;
        const auto endRowIdx =
            vectorIdx == endVectorIdx ? endRowIdxInVector : DEFAULT_VECTOR_CAPACITY;
        const auto numRowsInVector = endRowIdx - startRowIdx;
        numAppended += vectorVersionInfo.append(transaction->getID(), startRowIdx, numRowsInVector);
        std::vector<row_idx_t> rows;
        rows.resize(numRowsInVector);
        std::iota(rows.begin(), rows.end(), startRowIdx);
        if (transaction->getID() > 0) {
            transaction->pushVectorInsertInfo(*this, vectorIdx, vectorVersionInfo, rows);
        }
    }
    return numAppended;
}

bool NodeGroupVersionInfo::delete_(const transaction::Transaction* transaction,
    const row_idx_t rowIdx) {
    auto [vectorIdx, rowIdxInVector] =
        StorageUtils::getQuotientRemainder(rowIdx, DEFAULT_VECTOR_CAPACITY);
    auto& vectorVersionInfo = getVersionInfo(vectorIdx);
    const auto deleted = vectorVersionInfo.delete_(transaction->getID(), rowIdxInVector);
    if (deleted && transaction->getID() > 0) {
        transaction->pushVectorDeleteInfo(*this, vectorIdx, vectorVersionInfo, {rowIdxInVector});
    }
    return deleted;
}

void NodeGroupVersionInfo::getSelVectorToScan(const transaction_t startTS,
    const transaction_t transactionID, SelectionVector& selVector, const row_idx_t startRow,
    const row_idx_t numRows) const {
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

void NodeGroupVersionInfo::clearVectorInfo(const idx_t vectorIdx) {
    KU_ASSERT(vectorIdx < vectorsInfo.size());
    vectorsInfo[vectorIdx] = nullptr;
}

} // namespace storage
} // namespace kuzu
