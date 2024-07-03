#pragma once

#include <array>

#include "common/constants.h"
#include "common/copy_constructors.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

struct VectorVersionInfo {
    std::array<common::transaction_t, common::DEFAULT_VECTOR_CAPACITY> insertedVersions;
    std::array<common::transaction_t, common::DEFAULT_VECTOR_CAPACITY> deletedVersions;
    bool anyInserted;
    bool anyDeleted;
    // TODO: Keep an additional same insertion/deletion field.

    VectorVersionInfo()
        : insertedVersions{}, deletedVersions{}, anyInserted{false}, anyDeleted{false} {
        insertedVersions.fill(common::INVALID_TRANSACTION);
        deletedVersions.fill(common::INVALID_TRANSACTION);
    }
    DELETE_COPY_DEFAULT_MOVE(VectorVersionInfo);

    bool anyVersions() const { return anyInserted || anyDeleted; }
    common::row_idx_t append(common::transaction_t transactionID, common::row_idx_t startRow,
        common::row_idx_t numRows);
    bool delete_(common::transaction_t transactionID, common::row_idx_t rowIdx);

    void getSelVectorForScan(common::transaction_t startTS, common::transaction_t transactionID,
        common::SelectionVector& selVector, common::row_idx_t startRow, common::row_idx_t numRows,
        common::sel_t startOutputPos) const;

    void serialize(common::Serializer& serializer) const;

    common::row_idx_t numCommittedDeletions(const transaction::Transaction* transaction) const;

    // Given startTS and transactionID, if the row is deleted to the transaction, return true.
    bool isDeleted(common::transaction_t startTS, common::transaction_t transactionID,
        common::row_idx_t rowIdx) const;
    // Given startTS and transactionID, if the row is readable to the transaction, return true.
    bool isInserted(common::transaction_t startTS, common::transaction_t transactionID,
        common::row_idx_t rowIdx) const;

    common::row_idx_t getNumDeletions(common::transaction_t startTS,
        common::transaction_t transactionID, common::row_idx_t startRow,
        common::length_t numRows) const;
};

class VersionInfo {
public:
    VersionInfo() {}

    common::row_idx_t append(const transaction::Transaction* transaction,
        common::row_idx_t startRow, common::row_idx_t numRows);
    bool delete_(const transaction::Transaction* transaction, common::row_idx_t rowIdx);

    void getSelVectorToScan(common::transaction_t startTS, common::transaction_t transactionID,
        common::SelectionVector& selVector, common::row_idx_t startRow,
        common::row_idx_t numRows) const;

    void clearVectorInfo(common::idx_t vectorIdx);

    bool hasDeletions() const;
    bool getNumDeletions(const transaction::Transaction* transaction, common::row_idx_t startRow,
        common::length_t numRows) const;
    bool hasInsertions() const;
    bool isDeleted(const transaction::Transaction* transaction, common::row_idx_t rowInChunk) const;

    common::row_idx_t getNumDeletions(const transaction::Transaction* transaction) const;

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<VersionInfo> deserialize(common::Deserializer& deSer);

private:
    VectorVersionInfo& getOrCreateVersionInfo(common::idx_t vectorIdx);
    const VectorVersionInfo& getVersionInfo(common::idx_t vectorIdx) const;

private:
    std::vector<std::unique_ptr<VectorVersionInfo>> vectorsInfo;
};

} // namespace storage
} // namespace kuzu
