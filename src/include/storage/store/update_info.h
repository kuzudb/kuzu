#pragma once

#include "common/constants.h"
#include "common/types/types.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu {
namespace storage {

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

    explicit VectorUpdateInfo(common::transaction_t transactionID)
        : version{transactionID}, rowsInVector{}, numRowsUpdated{0}, prev{nullptr}, next{nullptr},
          data{nullptr} {}
};

class UpdateInfo {
public:
    UpdateInfo() {}

    void update(transaction::Transaction* transaction, common::offset_t offsetInChunk,
        common::ValueVector& values);

private:
    VectorUpdateInfo& getVectorInfo(transaction::Transaction* transaction, common::idx_t idx,
        common::sel_t rowIdxInVector);

private:
    std::vector<std::unique_ptr<VectorUpdateInfo>> vectorsInfo;
};

} // namespace storage
} // namespace kuzu
