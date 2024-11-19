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
class ChunkedGroupUndoIterator;

using chunked_group_undo_op_t = void (
    ChunkedNodeGroup::*)(common::row_idx_t, common::row_idx_t, common::transaction_t);

using chunked_group_iterator_construct_t =
    std::function<std::unique_ptr<ChunkedGroupUndoIterator>(common::row_idx_t, common::row_idx_t,
        common::node_group_idx_t, common::transaction_t commitTS)>;

// Note: these iterators are not necessarily thread-safe when used on their own
class ChunkedGroupUndoIterator {
public:
    ChunkedGroupUndoIterator(NodeGroupCollection* nodeGroups, common::row_idx_t startRow,
        common::row_idx_t numRows, common::transaction_t commitTS)
        : startRow(startRow), numRows(numRows), commitTS(commitTS), nodeGroups(nodeGroups) {}

    virtual ~ChunkedGroupUndoIterator() = default;

    virtual void initRollbackInsert(const transaction::Transaction* /*transaction*/) {}
    virtual void finalizeRollbackInsert() {};
    virtual void iterate(chunked_group_undo_op_t undoFunc) = 0;

protected:
    common::row_idx_t startRow;
    common::row_idx_t numRows;
    common::transaction_t commitTS;

    NodeGroupCollection* nodeGroups;
};

} // namespace storage
} // namespace kuzu
