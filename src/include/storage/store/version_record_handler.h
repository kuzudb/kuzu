#pragma once

#include "common/types/types.h"
#include "storage/store/chunked_node_group.h"

namespace kuzu {

namespace transaction {
class Transaction;
}

namespace storage {
class NodeGroupCollection;
class VersionRecordHandler;

using version_record_handler_op_t = void (
    ChunkedNodeGroup::*)(common::row_idx_t, common::row_idx_t, common::transaction_t);

// Note: these iterators are not necessarily thread-safe when used on their own
class VersionRecordHandler {
public:
    VersionRecordHandler(NodeGroupCollection* nodeGroups, common::row_idx_t startRow,
        common::row_idx_t numRows, common::transaction_t commitTS)
        : startRow(startRow), numRows(numRows), commitTS(commitTS), nodeGroups(nodeGroups) {}

    virtual ~VersionRecordHandler() = default;

    virtual void applyFuncToChunkedGroups(version_record_handler_op_t func) = 0;

    virtual void rollbackInsert(const transaction::Transaction* /*transaction*/) {
        applyFuncToChunkedGroups(&ChunkedNodeGroup::rollbackInsert);
    }

protected:
    common::row_idx_t startRow;
    common::row_idx_t numRows;
    common::transaction_t commitTS;

    NodeGroupCollection* nodeGroups;
};

// Contains pointer to a table + any information needed by the table to construct a
// VersionRecordHandler
class VersionRecordHandlerSelector {
public:
    virtual ~VersionRecordHandlerSelector() = default;

    virtual std::unique_ptr<VersionRecordHandler> constructVersionRecordHandler(
        common::row_idx_t startRow, common::row_idx_t numRows, common::transaction_t commitTS,
        common::node_group_idx_t nodeGroupIdx) const = 0;
};

} // namespace storage
} // namespace kuzu
