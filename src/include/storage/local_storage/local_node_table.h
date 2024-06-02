#pragma once

#include "common/copy_constructors.h"
#include "storage/local_storage/hash_index_local_storage.h"
#include "storage/local_storage/local_table.h"
#include "storage/store/node_group_collection.h"

namespace kuzu {
namespace storage {

class ChunkedNodeGroup;
struct TableScanState;

struct TableReadState;
class LocalNodeTable final : public LocalTable {
public:
    explicit LocalNodeTable(Table& table);
    DELETE_COPY_DEFAULT_MOVE(LocalNodeTable);

    bool insert(TableInsertState& insertState) override;
    bool update(TableUpdateState& updateState) override;
    bool delete_(TableDeleteState& deleteState) override;

    common::row_idx_t getNumRows() { return nodeGroups.getNumRows(); }
    common::node_group_idx_t getNumNodeGroups() { return nodeGroups.getNumNodeGroups(); }

    const NodeGroup& getNodeGroup(common::node_group_idx_t nodeGroupIdx) {
        return nodeGroups.getNodeGroup(nodeGroupIdx);
    }
    const NodeGroupCollection& getNodeGroups() const { return nodeGroups; }

private:
    std::unique_ptr<OverflowFile> overflowFile;
    std::unique_ptr<OverflowFileHandle> overflowFileHandle;
    std::unique_ptr<LocalHashIndex> hashIndex;
    NodeGroupCollection nodeGroups;
};

} // namespace storage
} // namespace kuzu
