#pragma once

#include "common/enums/rel_direction.h"
#include "storage/local_storage/local_table.h"
#include "storage/store/csr_node_group.h"

namespace kuzu {
namespace storage {
class MemoryManager;

static constexpr common::column_id_t LOCAL_BOUND_NODE_ID_COLUMN_ID = 0;
static constexpr common::column_id_t LOCAL_NBR_NODE_ID_COLUMN_ID = 1;
static constexpr common::column_id_t LOCAL_REL_ID_COLUMN_ID = 2;

class RelTable;
struct TableScanState;
struct RelTableUpdateState;
class LocalRelTable final : public LocalTable {
public:
    static std::vector<common::LogicalType> getTypesForLocalRelTable(const RelTable& table);

    explicit LocalRelTable(Table& table);

    bool insert(transaction::Transaction* transaction, TableInsertState& state) override;
    bool update(transaction::Transaction* transaction, TableUpdateState& state) override;
    bool delete_(transaction::Transaction* transaction, TableDeleteState& state) override;
    bool addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState) override;
    uint64_t getEstimatedMemUsage() override;

    bool checkIfNodeHasRels(common::ValueVector* srcNodeIDVector,
        common::RelDataDirection direction) const;

    common::TableType getTableType() const override { return common::TableType::REL; }

    void initializeScan(TableScanState& state);
    bool scan(transaction::Transaction* transaction, TableScanState& state) const;

    void clear() override {
        localNodeGroup.reset();
        fwdIndex.clear();
        bwdIndex.clear();
    }
    bool isEmpty() const {
        KU_ASSERT(
            (fwdIndex.empty() && bwdIndex.empty()) || (!fwdIndex.empty() && !bwdIndex.empty()));
        return fwdIndex.empty();
    }

    common::column_id_t getNumColumns() const { return localNodeGroup->getDataTypes().size(); }
    const std::unordered_map<common::column_id_t, common::table_id_t>&
    getNodeOffsetColumns() const {
        return nodeOffsetColumns;
    }

    std::map<common::offset_t, row_idx_vec_t>& getFWDIndex() { return fwdIndex; }
    const std::map<common::offset_t, row_idx_vec_t>& getFWDIndex() const { return fwdIndex; }
    std::map<common::offset_t, row_idx_vec_t>& getBWDIndex() { return bwdIndex; }
    const std::map<common::offset_t, row_idx_vec_t>& getBWDIndex() const { return bwdIndex; }
    NodeGroup& getLocalNodeGroup() const { return *localNodeGroup; }

    static std::vector<common::column_id_t> rewriteLocalColumnIDs(
        common::RelDataDirection direction, const std::vector<common::column_id_t>& columnIDs);
    static common::column_id_t rewriteLocalColumnID(common::RelDataDirection direction,
        common::column_id_t columnID);

private:
    common::row_idx_t findMatchingRow(common::offset_t srcNodeOffset,
        common::offset_t dstNodeOffset, common::offset_t relOffset);

private:
    // We don't duplicate local rel tuples. Tuples are stored same as node tuples.
    // Chunks stored in local rel table are organized as follows:
    // [srcNodeID, dstNodeID, relID, property1, property2, ...]
    // All local rel tuples are stored in a single node group, and they are indexed by src/dst
    // NodeID.
    std::map<common::offset_t, row_idx_vec_t> fwdIndex;
    std::map<common::offset_t, row_idx_vec_t> bwdIndex;
    std::unique_ptr<NodeGroup> localNodeGroup;
    std::unordered_map<common::column_id_t, common::table_id_t> nodeOffsetColumns;
};

} // namespace storage
} // namespace kuzu
