#include "storage/store/version_info.h"

#include "common/exception/runtime.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

row_idx_t VectorVersionInfo::append(const transaction_t transactionID, const row_idx_t startRow,
    const row_idx_t numRows) {
    insertionStatus = InsertionStatus::CHECK_VERSION;
    for (auto i = 0u; i < numRows; i++) {
        KU_ASSERT(insertedVersions[startRow + i] == INVALID_TRANSACTION);
        insertedVersions[startRow + i] = transactionID;
    }
    return true;
}

bool VectorVersionInfo::delete_(const transaction_t transactionID, const row_idx_t rowIdx) {
    deletionStatus = DeletionStatus::CHECK_VERSION;
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
    const row_idx_t numRows, sel_t startOutputPos) const {
    auto numSelected = selVector.getSelSize();
    if (deletionStatus == DeletionStatus::NO_DELETED &&
        insertionStatus == InsertionStatus::ALWAYS_INSERTED) {
        for (auto i = 0u; i < numRows; i++) {
            selVector.getMultableBuffer()[numSelected++] = startOutputPos + i;
        }
    } else if (insertionStatus != InsertionStatus::NO_INSERTED) {
        for (auto i = 0u; i < numRows; i++) {
            if (const auto rowIdx = startRow + i; isInserted(startTS, transactionID, rowIdx) &&
                                                  !isDeleted(startTS, transactionID, rowIdx)) {
                selVector.getMultableBuffer()[numSelected++] = startOutputPos + i;
            }
        }
    }
    selVector.setToFiltered(numSelected);
}

bool VectorVersionInfo::isDeleted(const transaction_t startTS, const transaction_t transactionID,
    const row_idx_t rowIdx) const {
    switch (deletionStatus) {
    case DeletionStatus::NO_DELETED: {
        return false;
    }
    case DeletionStatus::CHECK_VERSION: {
        const auto deletion = deletedVersions[rowIdx];
        const auto isDeletedWithinSameTransaction = deletion == transactionID;
        const auto isDeletedByPrevCommittedTransaction = deletion <= startTS;
        return isDeletedWithinSameTransaction || isDeletedByPrevCommittedTransaction;
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

bool VectorVersionInfo::isInserted(const transaction_t startTS, const transaction_t transactionID,
    const row_idx_t rowIdx) const {
    switch (insertionStatus) {
    case InsertionStatus::ALWAYS_INSERTED: {
        return true;
    }
    case InsertionStatus::NO_INSERTED: {
        return false;
    }
    case InsertionStatus::CHECK_VERSION: {
        const auto insertion = insertedVersions[rowIdx];
        const auto isInsertedWithinSameTransaction = insertion == transactionID;
        const auto isInsertedByPrevCommittedTransaction = insertion <= startTS;
        return isInsertedWithinSameTransaction || isInsertedByPrevCommittedTransaction;
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

row_idx_t VectorVersionInfo::getNumDeletions(transaction_t startTS, transaction_t transactionID,
    row_idx_t startRow, length_t numRows) const {
    if (deletionStatus == DeletionStatus::NO_DELETED) {
        return 0;
    }
    row_idx_t numDeletions = 0u;
    for (auto i = 0u; i < numRows; i++) {
        numDeletions += isDeleted(startTS, transactionID, startRow + i);
    }
    return numDeletions;
}

void VectorVersionInfo::serialize(Serializer& serializer) const {
    KU_ASSERT(deletionStatus == DeletionStatus::CHECK_VERSION);
    for (const auto deleted : deletedVersions) {
        // Versions should be either INVALID_TRANSACTION or committed timestamps.
        KU_ASSERT(deleted == INVALID_TRANSACTION ||
                  deleted < transaction::Transaction::START_TRANSACTION_ID);
        KU_UNUSED(deleted);
    }
    // TODO(Guodong): We should just serialize isDeleted or not to save space instead of versions.
    serializer.serializeArray<transaction_t, DEFAULT_VECTOR_CAPACITY>(deletedVersions);
}

row_idx_t VectorVersionInfo::numCommittedDeletions(
    const transaction::Transaction* transaction) const {
    row_idx_t numDeletions = 0;
    for (auto i = 0u; i < deletedVersions.size(); i++) {
        numDeletions += isDeleted(transaction->getStartTS(), transaction->getID(), i);
    }
    return numDeletions;
}

VectorVersionInfo& VersionInfo::getOrCreateVersionInfo(idx_t vectorIdx) {
    if (vectorsInfo.size() <= vectorIdx) {
        vectorsInfo.resize(vectorIdx + 1);
    }
    if (!vectorsInfo[vectorIdx]) {
        // TODO(Guodong): Should populate the vector with committed insertions. Set to 0.
        vectorsInfo[vectorIdx] = std::make_unique<VectorVersionInfo>();
    }
    return *vectorsInfo[vectorIdx];
}

const VectorVersionInfo& VersionInfo::getVersionInfo(idx_t vectorIdx) const {
    KU_ASSERT(vectorIdx < vectorsInfo.size());
    KU_ASSERT(vectorsInfo[vectorIdx]);
    return *vectorsInfo[vectorIdx];
}

row_idx_t VersionInfo::append(const transaction::Transaction* transaction, const row_idx_t startRow,
    const row_idx_t numRows) {
    auto [startVectorIdx, startRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow + numRows, DEFAULT_VECTOR_CAPACITY);
    row_idx_t numAppended = 0u;
    for (auto vectorIdx = startVectorIdx; vectorIdx <= endVectorIdx; vectorIdx++) {
        auto& vectorVersionInfo = getOrCreateVersionInfo(vectorIdx);
        const auto startRowIdx = vectorIdx == startVectorIdx ? startRowIdxInVector : 0;
        const auto endRowIdx =
            vectorIdx == endVectorIdx ? endRowIdxInVector : DEFAULT_VECTOR_CAPACITY;
        const auto numRowsInVector = endRowIdx - startRowIdx;
        numAppended += vectorVersionInfo.append(transaction->getID(), startRowIdx, numRowsInVector);
        if (transaction->shouldAppendToUndoBuffer()) {
            transaction->pushVectorInsertInfo(*this, vectorIdx, startRowIdx, numRowsInVector);
        }
    }
    return numAppended;
}

bool VersionInfo::delete_(const transaction::Transaction* transaction, const row_idx_t rowIdx) {
    auto [vectorIdx, rowIdxInVector] =
        StorageUtils::getQuotientRemainder(rowIdx, DEFAULT_VECTOR_CAPACITY);
    auto& vectorVersionInfo = getOrCreateVersionInfo(vectorIdx);
    if (vectorVersionInfo.insertionStatus == VectorVersionInfo::InsertionStatus::NO_INSERTED) {
        // Note: The version info is newly created due to `delete_`. There is no newly inserted rows
        // in this vector, thus all are rows checkpointed. We set the insertion status to
        // ALWAYS_INSERTED to avoid checking the version in the future.
        vectorVersionInfo.insertionStatus = VectorVersionInfo::InsertionStatus::ALWAYS_INSERTED;
    }
    const auto deleted = vectorVersionInfo.delete_(transaction->getID(), rowIdxInVector);
    if (deleted && transaction->shouldAppendToUndoBuffer()) {
        transaction->pushVectorDeleteInfo(*this, vectorIdx, rowIdxInVector, 1);
    }
    return deleted;
}

void VersionInfo::getSelVectorToScan(const transaction_t startTS, const transaction_t transactionID,
    SelectionVector& selVector, const row_idx_t startRow, const row_idx_t numRows) const {
    auto [startVectorIdx, startRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endRowIdxInVector] =
        StorageUtils::getQuotientRemainder(startRow + numRows - 1, DEFAULT_VECTOR_CAPACITY);
    auto vectorIdx = startVectorIdx;
    selVector.setSelSize(0);
    sel_t outputPos = 0u;
    while (vectorIdx <= endVectorIdx) {
        const auto startRowIdx = vectorIdx == startVectorIdx ? startRowIdxInVector : 0;
        const auto endRowIdx =
            vectorIdx == endVectorIdx ? endRowIdxInVector : DEFAULT_VECTOR_CAPACITY - 1;
        const auto numRowsInVector = endRowIdx - startRowIdx + 1;
        if (vectorIdx >= vectorsInfo.size() || !vectorsInfo[vectorIdx]) {
            auto numSelected = selVector.getSelSize();
            for (auto i = 0u; i < numRowsInVector; i++) {
                selVector.getMultableBuffer()[numSelected++] = outputPos + i;
            }
            selVector.setToFiltered(numSelected);
        } else {
            auto& vectorVersionInfo = getVersionInfo(vectorIdx);
            vectorVersionInfo.getSelVectorForScan(startTS, transactionID, selVector, startRowIdx,
                numRowsInVector, outputPos);
        }
        outputPos += numRowsInVector;
        vectorIdx++;
    }
    KU_ASSERT(outputPos <= DEFAULT_VECTOR_CAPACITY);
}

void VersionInfo::clearVectorInfo(const idx_t vectorIdx) {
    KU_ASSERT(vectorIdx < vectorsInfo.size());
    vectorsInfo[vectorIdx] = nullptr;
}

bool VersionInfo::hasDeletions() const {
    for (auto& vectorInfo : vectorsInfo) {
        if (vectorInfo &&
            vectorInfo->deletionStatus == VectorVersionInfo::DeletionStatus::CHECK_VERSION) {
            return true;
        }
    }
    return false;
}

bool VersionInfo::getNumDeletions(const transaction::Transaction* transaction, row_idx_t startRow,
    length_t numRows) const {
    auto [startVector, startRowInVector] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endRowInVector] =
        StorageUtils::getQuotientRemainder(startRow + numRows, DEFAULT_VECTOR_CAPACITY);
    idx_t vectorIdx = startVector;
    row_idx_t numDeletions = 0u;
    while (vectorIdx <= endVectorIdx) {
        const auto rowInVector = vectorIdx == startVector ? startRowInVector : 0;
        const auto numRowsInVector =
            vectorIdx == endVectorIdx ? endRowInVector : DEFAULT_VECTOR_CAPACITY - rowInVector;
        if (vectorsInfo[vectorIdx]) {
            numDeletions += vectorsInfo[vectorIdx]->getNumDeletions(transaction->getStartTS(),
                transaction->getID(), rowInVector, numRowsInVector);
        }
        vectorIdx++;
    }
    return numDeletions;
}

bool VersionInfo::hasInsertions() const {
    for (auto& vectorInfo : vectorsInfo) {
        if (vectorInfo &&
            vectorInfo->insertionStatus == VectorVersionInfo::InsertionStatus::CHECK_VERSION) {
            return true;
        }
    }
    return false;
}

bool VersionInfo::isDeleted(const transaction::Transaction* transaction,
    row_idx_t rowInChunk) const {
    auto [vectorIdx, rowInVector] =
        StorageUtils::getQuotientRemainder(rowInChunk, DEFAULT_VECTOR_CAPACITY);
    KU_ASSERT(vectorIdx < vectorsInfo.size());
    if (vectorsInfo[vectorIdx]) {
        return vectorsInfo[vectorIdx]->isDeleted(transaction->getStartTS(), transaction->getID(),
            rowInVector);
    }
    return false;
}

bool VersionInfo::isInserted(const transaction::Transaction* transaction,
    row_idx_t rowInChunk) const {
    auto [vectorIdx, rowInVector] =
        StorageUtils::getQuotientRemainder(rowInChunk, DEFAULT_VECTOR_CAPACITY);
    KU_ASSERT(vectorIdx < vectorsInfo.size());
    if (vectorsInfo[vectorIdx]) {
        return vectorsInfo[vectorIdx]->isInserted(transaction->getStartTS(), transaction->getID(),
            rowInVector);
    }
    return true;
}

row_idx_t VersionInfo::getNumDeletions(const transaction::Transaction* transaction) const {
    row_idx_t numDeletions = 0;
    for (auto& vectorInfo : vectorsInfo) {
        if (vectorInfo) {
            numDeletions += vectorInfo->numCommittedDeletions(transaction);
        }
    }
    return numDeletions;
}

void VersionInfo::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("vectors_info_size");
    serializer.write<uint64_t>(vectorsInfo.size());
    for (auto i = 0u; i < vectorsInfo.size(); i++) {
        auto hasDeletion = vectorsInfo[i] && vectorsInfo[i]->deletionStatus ==
                                                 VectorVersionInfo::DeletionStatus::CHECK_VERSION;
        serializer.writeDebuggingInfo("has_deletion");
        serializer.write<bool>(hasDeletion);
        if (hasDeletion) {
            serializer.writeDebuggingInfo("vector_info");
            vectorsInfo[i]->serialize(serializer);
        }
    }
}

std::unique_ptr<VersionInfo> VersionInfo::deserialize(Deserializer& deSer) {
    std::string key;
    uint64_t vectorSize;
    deSer.validateDebuggingInfo(key, "vectors_info_size");
    deSer.deserializeValue<uint64_t>(vectorSize);
    auto versionInfo = std::make_unique<VersionInfo>();
    for (auto i = 0u; i < vectorSize; i++) {
        bool hasDeletion;
        deSer.validateDebuggingInfo(key, "has_deletion");
        deSer.deserializeValue<bool>(hasDeletion);
        if (hasDeletion) {
            auto vectorVersionInfo = std::make_unique<VectorVersionInfo>();
            deSer.validateDebuggingInfo(key, "vector_info");
            deSer.deserializeArray<transaction_t, DEFAULT_VECTOR_CAPACITY>(
                vectorVersionInfo->deletedVersions);
            versionInfo->vectorsInfo.push_back(std::move(vectorVersionInfo));
        } else {
            versionInfo->vectorsInfo.push_back(nullptr);
        }
    }
    return versionInfo;
}

} // namespace storage
} // namespace kuzu
