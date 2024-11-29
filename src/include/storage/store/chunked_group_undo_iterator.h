#pragma once

#include <functional>

#include "common/types/types.h"

namespace kuzu {

namespace transaction {
class Transaction;
}

namespace storage {
class ChunkedNodeGroup;
class NodeGroupCollection;
class VersionRecordHandler;

using version_record_handler_op_t = void (
    ChunkedNodeGroup::*)(common::row_idx_t, common::row_idx_t, common::transaction_t);

using version_record_handler_construct_t = std::function<std::unique_ptr<VersionRecordHandler>(
    common::row_idx_t, common::row_idx_t, common::node_group_idx_t, common::transaction_t)>;

// Note: these iterators are not necessarily thread-safe when used on their own
class VersionRecordHandler {
public:
    VersionRecordHandler(NodeGroupCollection* nodeGroups, common::row_idx_t startRow,
        common::row_idx_t numRows, common::transaction_t commitTS)
        : startRow(startRow), numRows(numRows), commitTS(commitTS), nodeGroups(nodeGroups) {}

    virtual ~VersionRecordHandler() = default;

    virtual void initRollbackInsert(const transaction::Transaction* /*transaction*/) {}
    virtual void finalizeRollbackInsert(){};
    virtual void applyFuncToChunkedGroups(version_record_handler_op_t func) = 0;

protected:
    common::row_idx_t startRow;
    common::row_idx_t numRows;
    common::transaction_t commitTS;

    NodeGroupCollection* nodeGroups;
};

class VersionRecordHandlerData {
public:
    virtual ~VersionRecordHandlerData() = default;

    virtual std::unique_ptr<VersionRecordHandler> constructVersionRecordHandler(
        common::row_idx_t startRow, common::row_idx_t numRows, common::transaction_t commitTS,
        common::node_group_idx_t nodeGroupIdx) const = 0;
};

} // namespace storage
} // namespace kuzu
