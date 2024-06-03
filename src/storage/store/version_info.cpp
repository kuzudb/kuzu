#include "storage/store/version_info.h"

#include "common/exception/runtime.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

row_idx_t VectorVersionInfo::append(transaction_t transactionID, row_idx_t startRow,
    row_idx_t numRows) {
    anyValidVersions = true;
    for (auto i = 0u; i < numRows; i++) {
        KU_ASSERT(insertedVersions[startRow + i] == INVALID_TRANSACTION);
        insertedVersions[startRow + i] = transactionID;
    }
    return true;
}

bool VectorVersionInfo::delete_(transaction_t transactionID, row_idx_t rowIdx) {
    anyValidVersions = true;
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

VectorVersionInfo& NodeGroupVersionInfo::getVersionInfo(vector_idx_t vectorIdx) {
    if (vectorsInfo.size() <= vectorIdx) {
        vectorsInfo.resize(vectorIdx + 1);
    }
    if (!vectorsInfo[vectorIdx]) {
        vectorsInfo[vectorIdx] = std::make_unique<VectorVersionInfo>();
    }
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
    auto [vectorIdx, offsetInVector] =
        StorageUtils::getQuotientRemainder(rowIdx, DEFAULT_VECTOR_CAPACITY);
    auto& vectorVersionInfo = getVersionInfo(vectorIdx);
    return vectorVersionInfo.delete_(transactionID, offsetInVector);
}

} // namespace storage
} // namespace kuzu
