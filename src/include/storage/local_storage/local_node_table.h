#pragma once

#include "common/copy_constructors.h"
#include "storage/local_storage/hash_index_local_storage.h"
#include "storage/local_storage/local_table.h"

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

    void initializeScanState(TableScanState& scanState) const;
    void scan(TableScanState& scanState) const;

    common::row_idx_t getNumRows() const { return chunkedGroups.getNumRows(); }

    const ChunkedNodeGroupCollection& getChunkedGroups() const { return chunkedGroups; }

private:
    std::unique_ptr<OverflowFile> overflowFile;
    std::unique_ptr<OverflowFileHandle> overflowFileHandle;
    std::unique_ptr<LocalHashIndex> hashIndex;
    ChunkedNodeGroupCollection chunkedGroups;
};

} // namespace storage
} // namespace kuzu
