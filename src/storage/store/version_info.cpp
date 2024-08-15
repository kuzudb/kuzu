#include "storage/store/version_info.h"

#include <algorithm>

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

void VectorVersionInfo::rollbackInsertions(row_idx_t startRowInVector, row_idx_t numRows) {
    for (auto row = startRowInVector; row < startRowInVector + numRows; row++) {
        insertedVersions[row] = INVALID_TRANSACTION;
    }
    // TODO(Guodong): We can choose to vaccum inserted key/values in this transaction from
    // index.
    bool hasAnyInsertions = false;
    for (const auto& version : insertedVersions) {
        if (version != INVALID_TRANSACTION) {
            hasAnyInsertions = true;
            break;
        }
    }
    if (!hasAnyInsertions) {
        insertionStatus = InsertionStatus::NO_INSERTED;
        deletionStatus = DeletionStatus::NO_DELETED;
    }
}

void VectorVersionInfo::rollbackDeletions(row_idx_t startRowInVector, row_idx_t numRows) {
    for (auto row = startRowInVector; row < startRowInVector + numRows; row++) {
        deletedVersions[row] = INVALID_TRANSACTION;
    }
    bool hasAnyDeletions = false;
    for (const auto& version : deletedVersions) {
        if (version != INVALID_TRANSACTION) {
            hasAnyDeletions = true;
            break;
        }
    }
    if (!hasAnyDeletions) {
        deletionStatus = DeletionStatus::NO_DELETED;
    }
}

bool VectorVersionInfo::finalizeStatusFromVersions() {
    if (insertionStatus == InsertionStatus::NO_INSERTED) {
        KU_ASSERT(deletionStatus == VectorVersionInfo::DeletionStatus::NO_DELETED);
        return true;
    }
    const auto hasAnyDeletions =
        std::any_of(deletedVersions.begin(), deletedVersions.end(), [](auto version) {
            KU_ASSERT(version == 0 || version == INVALID_TRANSACTION);
            return version == 0;
        });
    if (!hasAnyDeletions) {
        deletionStatus = DeletionStatus::NO_DELETED;
    }
    const auto allValidInsertions =
        std::all_of(insertedVersions.begin(), insertedVersions.end(), [](auto version) {
            KU_ASSERT(version == 0 || version == INVALID_TRANSACTION);
            return version == 0;
        });
    if (allValidInsertions) {
        insertionStatus = InsertionStatus::ALWAYS_INSERTED;
    } else {
        const auto hasAnyValidInsertions =
            std::any_of(insertedVersions.begin(), insertedVersions.end(), [](auto version) {
                KU_ASSERT(version == 0 || version == INVALID_TRANSACTION);
                return version == 0;
            });
        if (!hasAnyValidInsertions) {
            insertionStatus = InsertionStatus::NO_INSERTED;
        } else {
            insertionStatus = InsertionStatus::CHECK_VERSION;
        }
    }
    if (insertionStatus == InsertionStatus::ALWAYS_INSERTED &&
        deletionStatus == DeletionStatus::NO_DELETED) {
        // No need to keep vector info as all tuples are valid and no deletions.
        return false;
    }
    return true;
}

void VectorVersionInfo::serialize(Serializer& serializer) const {
    for (const auto inserted : insertedVersions) {
        // Versions should be either INVALID_TRANSACTION or committed timestamps.
        KU_ASSERT(inserted == INVALID_TRANSACTION ||
                  inserted < transaction::Transaction::START_TRANSACTION_ID);
        KU_UNUSED(inserted);
    }
    for (const auto deleted : deletedVersions) {
        // Versions should be either INVALID_TRANSACTION or committed timestamps.
        KU_ASSERT(deleted == INVALID_TRANSACTION ||
                  deleted < transaction::Transaction::START_TRANSACTION_ID);
        KU_UNUSED(deleted);
    }
    serializer.writeDebuggingInfo("insertion_status");
    serializer.serializeValue<InsertionStatus>(insertionStatus);
    serializer.writeDebuggingInfo("deletion_status");
    serializer.serializeValue<DeletionStatus>(deletionStatus);
    switch (insertionStatus) {
    case InsertionStatus::NO_INSERTED:
    case InsertionStatus::ALWAYS_INSERTED: {
        // Nothing to serialize.
    } break;
    case InsertionStatus::CHECK_VERSION: {
        serializer.writeDebuggingInfo("inserted_versions");
        serializer.serializeArray<transaction_t, DEFAULT_VECTOR_CAPACITY>(insertedVersions);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    switch (deletionStatus) {
    case DeletionStatus::NO_DELETED: {
        // Nothing to serialize.
    } break;
    case DeletionStatus::CHECK_VERSION: {
        serializer.writeDebuggingInfo("deleted_versions");
        serializer.serializeArray<transaction_t, DEFAULT_VECTOR_CAPACITY>(deletedVersions);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::unique_ptr<VectorVersionInfo> VectorVersionInfo::deSerialize(Deserializer& deSer) {
    std::string key;
    InsertionStatus insertionStatus;
    DeletionStatus deletionStatus;
    deSer.validateDebuggingInfo(key, "insertion_status");
    deSer.deserializeValue<InsertionStatus>(insertionStatus);
    deSer.validateDebuggingInfo(key, "deletion_status");
    deSer.deserializeValue<DeletionStatus>(deletionStatus);
    auto vectorVersionInfo = std::make_unique<VectorVersionInfo>();
    vectorVersionInfo->insertionStatus = insertionStatus;
    vectorVersionInfo->deletionStatus = deletionStatus;
    switch (vectorVersionInfo->insertionStatus) {
    case InsertionStatus::NO_INSERTED:
    case InsertionStatus::ALWAYS_INSERTED: {
        // Nothing to deserialize.
    } break;
    case InsertionStatus::CHECK_VERSION: {
        deSer.validateDebuggingInfo(key, "inserted_versions");
        deSer.deserializeArray<transaction_t, DEFAULT_VECTOR_CAPACITY>(
            vectorVersionInfo->insertedVersions);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    switch (vectorVersionInfo->deletionStatus) {
    case DeletionStatus::NO_DELETED: {
        // Nothing to deserialize.
    } break;
    case DeletionStatus::CHECK_VERSION: {
        deSer.validateDebuggingInfo(key, "deleted_versions");
        deSer.deserializeArray<transaction_t, DEFAULT_VECTOR_CAPACITY>(
            vectorVersionInfo->deletedVersions);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    for (const auto inserted : vectorVersionInfo->insertedVersions) {
        // Versions should be either INVALID_TRANSACTION or committed timestamps.
        KU_ASSERT(inserted == INVALID_TRANSACTION ||
                  inserted < transaction::Transaction::START_TRANSACTION_ID);
        KU_UNUSED(inserted);
    }
    for (const auto deleted : vectorVersionInfo->deletedVersions) {
        // Versions should be either INVALID_TRANSACTION or committed timestamps.
        KU_ASSERT(deleted == INVALID_TRANSACTION ||
                  deleted < transaction::Transaction::START_TRANSACTION_ID);
        KU_UNUSED(deleted);
    }
    return vectorVersionInfo;
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
        vectorsInfo[vectorIdx] = std::make_unique<VectorVersionInfo>();
    }
    return *vectorsInfo[vectorIdx];
}

VectorVersionInfo* VersionInfo::getVectorVersionInfo(idx_t vectorIdx) const {
    if (vectorIdx >= vectorsInfo.size()) {
        return nullptr;
    }
    return vectorsInfo[vectorIdx].get();
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
        const auto vectorVersion = getVectorVersionInfo(vectorIdx);
        if (!vectorVersion) {
            auto numSelected = selVector.getSelSize();
            for (auto i = 0u; i < numRowsInVector; i++) {
                selVector.getMultableBuffer()[numSelected++] = outputPos + i;
            }
            selVector.setToFiltered(numSelected);
        } else {
            vectorVersion->getSelVectorForScan(startTS, transactionID, selVector, startRowIdx,
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

row_idx_t VersionInfo::getNumDeletions(const transaction::Transaction* transaction,
    row_idx_t startRow, length_t numRows) const {
    auto [startVector, startRowInVector] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endRowInVector] =
        StorageUtils::getQuotientRemainder(startRow + numRows, DEFAULT_VECTOR_CAPACITY);
    idx_t vectorIdx = startVector;
    row_idx_t numDeletions = 0u;
    while (vectorIdx <= endVectorIdx) {
        const auto rowInVector = vectorIdx == startVector ? startRowInVector : 0;
        const auto numRowsInVector =
            (vectorIdx == endVectorIdx ? endRowInVector : DEFAULT_VECTOR_CAPACITY) - rowInVector;
        const auto vectorVersion = getVectorVersionInfo(vectorIdx);
        if (vectorVersion) {
            numDeletions += vectorVersion->getNumDeletions(transaction->getStartTS(),
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
    const auto vectorVersion = getVectorVersionInfo(vectorIdx);
    if (vectorVersion) {
        return vectorVersion->isDeleted(transaction->getStartTS(), transaction->getID(),
            rowInVector);
    }
    return false;
}

bool VersionInfo::isInserted(const transaction::Transaction* transaction,
    row_idx_t rowInChunk) const {
    auto [vectorIdx, rowInVector] =
        StorageUtils::getQuotientRemainder(rowInChunk, DEFAULT_VECTOR_CAPACITY);
    const auto vectorVersion = getVectorVersionInfo(vectorIdx);
    if (vectorVersion) {
        return vectorVersion->isInserted(transaction->getStartTS(), transaction->getID(),
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

bool VersionInfo::finalizeStatusFromVersions() {
    for (auto vectorIdx = 0u; vectorIdx < getNumVectors(); vectorIdx++) {
        const auto vectorInfo = getVectorVersionInfo(vectorIdx);
        if (!vectorInfo) {
            continue;
        }
        if (!vectorInfo->finalizeStatusFromVersions()) {
            clearVectorInfo(vectorIdx);
        }
    }
    bool hasAnyVectorVersionInfo = false;
    for (const auto& info : vectorsInfo) {
        if (info) {
            hasAnyVectorVersionInfo = true;
            break;
        }
    }
    return hasAnyVectorVersionInfo;
}

void VersionInfo::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("vectors_info_size");
    serializer.write<uint64_t>(vectorsInfo.size());
    for (auto i = 0u; i < vectorsInfo.size(); i++) {
        auto hasVectorVersion = vectorsInfo[i] != nullptr;
        serializer.writeDebuggingInfo("has_vector_info");
        serializer.write<bool>(hasVectorVersion);
        if (hasVectorVersion) {
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
        bool hasVectorVersion;
        deSer.validateDebuggingInfo(key, "has_vector_info");
        deSer.deserializeValue<bool>(hasVectorVersion);
        if (hasVectorVersion) {
            deSer.validateDebuggingInfo(key, "vector_info");
            auto vectorVersionInfo = VectorVersionInfo::deSerialize(deSer);
            versionInfo->vectorsInfo.push_back(std::move(vectorVersionInfo));
        } else {
            versionInfo->vectorsInfo.push_back(nullptr);
        }
    }
    return versionInfo;
}

} // namespace storage
} // namespace kuzu
