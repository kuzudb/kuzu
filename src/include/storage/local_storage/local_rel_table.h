#pragma once

#include <map>

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
    using rel_index_t = std::map<common::offset_t, row_idx_vec_t>;

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
    common::row_idx_t getNumTotalRows() override { return localNodeGroup->getNumRows(); }

    rel_index_t& getFWDIndex() { return fwdIndex; }
    rel_index_t& getBWDIndex() { return bwdIndex; }
    NodeGroup& getLocalNodeGroup() const { return *localNodeGroup; }

    static std::vector<common::column_id_t> rewriteLocalColumnIDs(
        common::RelDataDirection direction, const std::vector<common::column_id_t>& columnIDs);
    static common::column_id_t rewriteLocalColumnID(common::RelDataDirection direction,
        common::column_id_t columnID);

private:
    common::row_idx_t findMatchingRow(transaction::Transaction* transaction,
        common::offset_t srcNodeOffset, common::offset_t dstNodeOffset, common::offset_t relOffset);

private:
    // We don't duplicate local rel tuples. Tuples are stored same as node tuples.
    // Chunks stored in local rel table are organized as follows:
    // [srcNodeID, dstNodeID, relID, property1, property2, ...]
    // All local rel tuples are stored in a single node group, and they are indexed by src/dst
    // NodeID.
    rel_index_t fwdIndex;
    rel_index_t bwdIndex;
    std::unique_ptr<NodeGroup> localNodeGroup;
};

} // namespace storage
} // namespace kuzu
