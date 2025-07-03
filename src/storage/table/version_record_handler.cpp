#include "storage/table/version_record_handler.h"

#include "main/client_context.h"
#include "storage/table/chunked_node_group.h"

namespace kuzu::storage {

void VersionRecordHandler::rollbackInsert(main::ClientContext* context,
    common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
    common::row_idx_t numRows) const {
    applyFuncToChunkedGroups(&ChunkedNodeGroup::rollbackInsert, nodeGroupIdx, startRow, numRows,
        context->getTransaction()->getCommitTS());
}

} // namespace kuzu::storage
