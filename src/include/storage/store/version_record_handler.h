#pragma once

#include "common/types/types.h"
#include "transaction/transaction.h"

namespace kuzu {

namespace storage {

class ChunkedNodeGroup;

using version_record_handler_op_t = void (
    ChunkedNodeGroup::*)(common::row_idx_t, common::row_idx_t, common::transaction_t);

// Note: these handlers are not safe to use in multi-threaded contexts without external locking
class VersionRecordHandler {
public:
    virtual ~VersionRecordHandler() = default;

    virtual void applyFuncToChunkedGroups(version_record_handler_op_t func,
        common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows, common::transaction_t commitTS) const = 0;

    virtual void rollbackInsert(const transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows) const;
};

} // namespace storage
} // namespace kuzu
