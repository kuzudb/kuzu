#pragma once

#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

struct VectorVersionInfo {
    std::array<common::transaction_t, common::DEFAULT_VECTOR_CAPACITY> insertedVersions;
    std::array<common::transaction_t, common::DEFAULT_VECTOR_CAPACITY> deletedVersions;
    bool anyValidVersions;

    VectorVersionInfo() : insertedVersions{}, deletedVersions{}, anyValidVersions{false} {
        insertedVersions.fill(common::INVALID_TRANSACTION);
        deletedVersions.fill(common::INVALID_TRANSACTION);
    }

    bool anyVersions() const { return anyValidVersions; }
    common::row_idx_t append(common::transaction_t transactionID, common::row_idx_t startRow,
        common::row_idx_t numRows);
    bool delete_(common::transaction_t transactionID, common::row_idx_t rowIdx);
};

class NodeGroupVersionInfo {
public:
    NodeGroupVersionInfo() {}

    common::row_idx_t append(common::transaction_t transactionID, common::row_idx_t startRow,
        common::row_idx_t numRows);
    bool delete_(common::transaction_t transactionID, common::row_idx_t rowIdx);

private:
    VectorVersionInfo& getVersionInfo(common::vector_idx_t vectorIdx);

private:
    std::vector<std::unique_ptr<VectorVersionInfo>> vectorsInfo;
};

} // namespace storage
} // namespace kuzu
