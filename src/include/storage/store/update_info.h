#pragma once

#include <array>

#include "column_chunk_data.h"
#include "common/constants.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {
class ValueVector;
} // namespace common

namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {
class MemoryManager;

class ColumnChunkData;
struct VectorUpdateInfo {
    common::transaction_t version;
    std::array<common::sel_t, common::DEFAULT_VECTOR_CAPACITY> rowsInVector;
    common::sel_t numRowsUpdated;
    // Older versions.
    std::unique_ptr<VectorUpdateInfo> prev;
    // Newer versions.
    VectorUpdateInfo* next;

    std::unique_ptr<ColumnChunkData> data;

    explicit VectorUpdateInfo(MemoryManager& memoryManager,
        const common::transaction_t transactionID, common::LogicalType dataType)
        : version{transactionID}, rowsInVector{}, numRowsUpdated{0}, prev{nullptr}, next{nullptr} {
        data = ColumnChunkFactory::createColumnChunkData(memoryManager, std::move(dataType), false,
            common::DEFAULT_VECTOR_CAPACITY, ResidencyState::IN_MEMORY);
    }

    std::unique_ptr<VectorUpdateInfo> movePrev() { return std::move(prev); }
    void setPrev(std::unique_ptr<VectorUpdateInfo> prev) { this->prev = std::move(prev); }
    VectorUpdateInfo* getPrev() const { return prev.get(); }
    void setNext(VectorUpdateInfo* next) { this->next = next; }
    VectorUpdateInfo* getNext() const { return next; }
};

class UpdateInfo {
public:
    UpdateInfo() {}

    VectorUpdateInfo* update(MemoryManager& memoryManager,
        const transaction::Transaction* transaction, common::idx_t vectorIdx,
        common::sel_t rowIdxInVector, const common::ValueVector& values);

    void setVectorInfo(common::idx_t vectorIdx, std::unique_ptr<VectorUpdateInfo> vectorInfo) {
        vectorsInfo[vectorIdx] = std::move(vectorInfo);
    }
    void clearVectorInfo(common::idx_t vectorIdx) { vectorsInfo[vectorIdx] = nullptr; }

    common::idx_t getNumVectors() const { return vectorsInfo.size(); }
    VectorUpdateInfo* getVectorInfo(const transaction::Transaction* transaction,
        common::idx_t idx) const;

    common::row_idx_t getNumUpdatedRows(const transaction::Transaction* transaction) const;

    bool hasUpdates(const transaction::Transaction* transaction, common::row_idx_t startRow,
        common::length_t numRows) const;

private:
    VectorUpdateInfo& getOrCreateVectorInfo(MemoryManager& memoryManager,
        const transaction::Transaction* transaction, common::idx_t vectorIdx,
        common::sel_t rowIdxInVector, const common::LogicalType& dataType);

private:
    std::vector<std::unique_ptr<VectorUpdateInfo>> vectorsInfo;
};

} // namespace storage
} // namespace kuzu
