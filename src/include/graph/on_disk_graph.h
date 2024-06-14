#pragma once

#include "graph.h"
#include "graph_entry.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace graph {

struct OnDiskGraphScanState {
    std::shared_ptr<common::DataChunkState> srcNodeIDVectorState;
    std::shared_ptr<common::DataChunkState> dstNodeIDVectorState;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;

    std::unique_ptr<storage::RelTableScanState> fwdScanState;
    std::unique_ptr<storage::RelTableScanState> bwdScanState;

    explicit OnDiskGraphScanState(storage::MemoryManager* mm);
};

class OnDiskGraph final : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, const GraphEntry& entry);

    std::vector<common::table_id_t> getNodeTableIDs() override;
    std::vector<common::table_id_t> getRelTableIDs() override;

    common::offset_t getNumNodes() override;
    common::offset_t getNumNodes(common::table_id_t id) override;

    std::vector<common::nodeID_t> scanFwd(common::nodeID_t nodeID) override;
    std::vector<common::nodeID_t> scanFwd(common::nodeID_t nodeID,
        common::table_id_t relTableID) override;

    std::vector<common::nodeID_t> scanBwd(common::nodeID_t nodeID) override;
    std::vector<common::nodeID_t> scanBwd(common::nodeID_t nodeID,
        common::table_id_t relTableID) override;

private:
    void scan(common::nodeID_t nodeID, storage::RelTable* relTable,
        storage::RelTableScanState& relTableScanState, std::vector<common::nodeID_t>& nbrNodeIDs);

private:
    transaction::Transaction* transaction;
    GraphEntry graphEntry;
    common::table_id_map_t<storage::NodeTable*> nodeIDToNodeTable;
    common::table_id_map_t<common::table_id_map_t<storage::RelTable*>> nodeTableIDToFwdRelTables;
    common::table_id_map_t<common::table_id_map_t<storage::RelTable*>> nodeTableIDToBwdRelTables;
    OnDiskGraphScanState scanState;
};

} // namespace graph
} // namespace kuzu
