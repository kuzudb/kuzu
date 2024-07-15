#pragma once

#include "common/enums/rel_direction.h"
#include "common/vector/value_vector.h"
#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace storage {

static constexpr common::column_id_t LOCAL_BOUND_NODE_ID_COLUMN_ID = 0;
static constexpr common::column_id_t LOCAL_NBR_NODE_ID_COLUMN_ID = 1;
static constexpr common::column_id_t LOCAL_REL_ID_COLUMN_ID = 2;

class RelTable;
struct RelTableUpdateState;
class LocalRelTable final : public LocalTable {
public:
    static std::vector<common::LogicalType> getTypesForLocalRelTable(const RelTable& table);

    explicit LocalRelTable(Table& table);

    bool insert(transaction::Transaction* transaction, TableInsertState& state) override;
    bool update(TableUpdateState& state) override;
    bool delete_(transaction::Transaction* transaction, TableDeleteState& state) override;
    bool addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState) override;

    void checkIfNodeHasRels(common::ValueVector* srcNodeIDVector) const;

    common::TableType getTableType() const override { return common::TableType::REL; }

    void initializeScan(TableScanState& state);
    bool scan(transaction::Transaction* transaction, TableScanState& state) const;

    void clear() override {
        localNodeGroup.reset();
        fwdIndex.clear();
        bwdIndex.clear();
    }

    common::column_id_t getNumColumns() const { return localNodeGroup->getDataTypes().size(); }

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
    // [srcNodeID, dstNodeID, property1, property2, ...]
    // All local rel tuples are stored in a single node group, and they are indexed by src/dst
    // NodeID.
    std::map<common::offset_t, row_idx_vec_t> fwdIndex;
    std::map<common::offset_t, row_idx_vec_t> bwdIndex;
    std::unique_ptr<NodeGroup> localNodeGroup;
};

} // namespace storage
} // namespace kuzu
