#pragma once

#include <array>

#include "common/constants.h"
#include "common/copy_constructors.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

struct VectorVersionInfo {
    enum class InsertionStatus : uint8_t { NO_INSERTED, CHECK_VERSION, ALWAYS_INSERTED };
    // TODO(Guodong): ALWAYS_INSERTED is not added for now, but it may be useful as an optimization
    // to mark the vector data after checkpoint is all deleted.
    enum class DeletionStatus : uint8_t { NO_DELETED, CHECK_VERSION };

    // TODO: Keep an additional same insertion/deletion field as an optimization to avoid the need
    // of `array` if all are inserted/deleted in the same transaction.
    // Also, avoid allocate `array` when status are NO_INSERTED and NO_DELETED.
    // We can even consider separating the insertion and deletion into two separate Vectors.
    std::unique_ptr<std::array<common::transaction_t, common::DEFAULT_VECTOR_CAPACITY>>
        insertedVersions;
    std::unique_ptr<std::array<common::transaction_t, common::DEFAULT_VECTOR_CAPACITY>>
        deletedVersions;
    // If all values in the Vector are inserted/deleted in the same transaction, we can use this to
    // aovid the allocation of `array`.
    common::transaction_t sameInsertionVersion;
    common::transaction_t sameDeletionVersion;
    InsertionStatus insertionStatus;
    DeletionStatus deletionStatus;

    VectorVersionInfo()
        : sameInsertionVersion{common::INVALID_TRANSACTION},
          sameDeletionVersion{common::INVALID_TRANSACTION},
          insertionStatus{InsertionStatus::NO_INSERTED},
          deletionStatus{DeletionStatus::NO_DELETED} {}
    DELETE_COPY_DEFAULT_MOVE(VectorVersionInfo);

    void append(common::transaction_t transactionID, common::row_idx_t startRow,
        common::row_idx_t numRows);
    bool delete_(common::transaction_t transactionID, common::row_idx_t rowIdx);
    void setInsertCommitTS(common::transaction_t commitTS, common::row_idx_t startRow,
        common::row_idx_t numRows);
    void setDeleteCommitTS(common::transaction_t commitTS, common::row_idx_t startRow,
        common::row_idx_t numRows);

    void getSelVectorForScan(common::transaction_t startTS, common::transaction_t transactionID,
        common::SelectionVector& selVector, common::row_idx_t startRow, common::row_idx_t numRows,
        common::sel_t startOutputPos) const;

    void rollbackInsertions(common::row_idx_t startRowInVector, common::row_idx_t numRows);
    void rollbackDeletions(common::row_idx_t startRowInVector, common::row_idx_t numRows);

    bool hasDeletions(const transaction::Transaction* transaction) const;

    // Given startTS and transactionID, if the row is deleted to the transaction, return true.
    bool isDeleted(common::transaction_t startTS, common::transaction_t transactionID,
        common::row_idx_t rowIdx) const;
    // Given startTS and transactionID, if the row is readable to the transaction, return true.
    bool isInserted(common::transaction_t startTS, common::transaction_t transactionID,
        common::row_idx_t rowIdx) const;

    common::row_idx_t getNumDeletions(common::transaction_t startTS,
        common::transaction_t transactionID, common::row_idx_t startRow,
        common::length_t numRows) const;

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<VectorVersionInfo> deSerialize(common::Deserializer& deSer);

private:
    void initInsertionVersionArray();
    void initDeletionVersionArray();

    bool isSameInsertionVersion() const;
    bool isSameDeletionVersion() const;
};

class ChunkedNodeGroup;
class VersionInfo {
public:
    VersionInfo() {}

    void append(common::transaction_t transactionID, common::row_idx_t startRow,
        common::row_idx_t numRows);
    bool delete_(common::transaction_t transactionID, common::row_idx_t rowIdx);

    void getSelVectorToScan(common::transaction_t startTS, common::transaction_t transactionID,
        common::SelectionVector& selVector, common::row_idx_t startRow,
        common::row_idx_t numRows) const;

    void clearVectorInfo(common::idx_t vectorIdx);

    bool hasDeletions() const;
    common::row_idx_t getNumDeletions(const transaction::Transaction* transaction,
        common::row_idx_t startRow, common::length_t numRows) const;
    bool hasInsertions() const;
    bool isDeleted(const transaction::Transaction* transaction, common::row_idx_t rowInChunk) const;
    bool isInserted(const transaction::Transaction* transaction,
        common::row_idx_t rowInChunk) const;

    bool hasDeletions(const transaction::Transaction* transaction) const;

    // Return nullptr when vectorIdx is out of range or when the vector is not created.
    VectorVersionInfo* getVectorVersionInfo(common::idx_t vectorIdx) const;
    common::idx_t getNumVectors() const { return vectorsInfo.size(); }
    VectorVersionInfo& getOrCreateVersionInfo(common::idx_t vectorIdx);

    void commitInsert(common::row_idx_t startRow, common::row_idx_t numRows,
        common::transaction_t commitTS);
    void rollbackInsert(common::row_idx_t startRow, common::row_idx_t numRows);
    void commitDelete(common::row_idx_t startRow, common::row_idx_t numRows,
        common::transaction_t commitTS);
    void rollbackDelete(common::row_idx_t startRow, common::row_idx_t numRows);

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<VersionInfo> deserialize(common::Deserializer& deSer);

private:
    std::vector<std::unique_ptr<VectorVersionInfo>> vectorsInfo;
};

} // namespace storage
} // namespace kuzu
