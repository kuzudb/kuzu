#include "storage/store/version_record_handler.h"

#include "storage/store/chunked_node_group.h"

namespace kuzu::storage {
void VersionRecordHandler::rollbackInsert(const transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
    common::row_idx_t numRows) const {
    applyFuncToChunkedGroups(&ChunkedNodeGroup::rollbackInsert, nodeGroupIdx, startRow, numRows,
        transaction->getCommitTS());
}
} // namespace kuzu::storage
