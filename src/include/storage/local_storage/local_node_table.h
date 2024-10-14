#pragma once

#include "common/copy_constructors.h"
#include "storage/local_storage/local_hash_index.h"
#include "storage/local_storage/local_table.h"
#include "storage/store/node_group_collection.h"

namespace kuzu {
namespace storage {

struct TableScanState;
class MemoryManager;

struct TableReadState;
class LocalNodeTable final : public LocalTable {
public:
    explicit LocalNodeTable(Table& table);
    DELETE_COPY_AND_MOVE(LocalNodeTable);

    bool insert(transaction::Transaction* transaction, TableInsertState& insertState) override;
    bool update(transaction::Transaction* transaction, TableUpdateState& updateState) override;
    bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) override;
    bool addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState) override;
    uint64_t getEstimatedMemUsage() override;

    common::offset_t validateUniquenessConstraint(const transaction::Transaction* transaction,
        const common::ValueVector& pkVector);

    common::TableType getTableType() const override { return common::TableType::NODE; }

    void clear() override;

    common::row_idx_t getNumRows() { return nodeGroups.getNumTotalRows(); }
    common::node_group_idx_t getNumNodeGroups() { return nodeGroups.getNumNodeGroups(); }

    NodeGroup* getNodeGroup(common::node_group_idx_t nodeGroupIdx) {
        return nodeGroups.getNodeGroup(nodeGroupIdx);
    }
    NodeGroupCollection& getNodeGroups() { return nodeGroups; }

    bool lookupPK(const transaction::Transaction* transaction, const common::ValueVector* keyVector,
        common::offset_t& result);

    TableStats getStats() const { return nodeGroups.getStats(); }

private:
    void initLocalHashIndex();
    bool isVisible(const transaction::Transaction* transaction, common::offset_t offset);

private:
    PageCursor overflowCursor;
    std::unique_ptr<OverflowFile> overflowFile;
    std::unique_ptr<OverflowFileHandle> overflowFileHandle;
    std::unique_ptr<LocalHashIndex> hashIndex;
    NodeGroupCollection nodeGroups;
};

} // namespace storage
} // namespace kuzu
