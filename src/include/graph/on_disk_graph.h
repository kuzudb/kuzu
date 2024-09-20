#pragma once

#include "common/vector/value_vector.h"
#include "graph.h"
#include "graph_entry.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {
class MemoryManager;
}
namespace graph {

struct OnDiskGraphScanState {
    std::unique_ptr<storage::RelTableScanState> fwdScanState;
    std::unique_ptr<storage::RelTableScanState> bwdScanState;

    explicit OnDiskGraphScanState(common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector);
};

class OnDiskGraphScanStates : public GraphScanState {
    friend class OnDiskGraph;

public:
    ~OnDiskGraphScanStates() override = default;

private:
    std::shared_ptr<common::DataChunkState> srcNodeIDVectorState;
    std::shared_ptr<common::DataChunkState> dstNodeIDVectorState;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;

    explicit OnDiskGraphScanStates(std::span<common::table_id_t> tableIDs,
        storage::MemoryManager* mm);
    std::vector<std::pair<common::table_id_t, OnDiskGraphScanState>> scanStates;
};

class OnDiskGraph final : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, const GraphEntry& entry);
    // TODO(Reviewer): Do I need to do other.graphEntry.copy() in the constructor?
    // TODO(Reviewer): I believe I have to make this copy constructor public. Can you double check?
    OnDiskGraph(const OnDiskGraph& other);

    std::unique_ptr<Graph> copy() override { return std::make_unique<OnDiskGraph>(*this); }
    std::vector<common::table_id_t> getNodeTableIDs() override;
    std::vector<common::table_id_t> getRelTableIDs() override;

    common::offset_t getNumNodes() override;
    common::offset_t getNumNodes(common::table_id_t id) override;

    std::vector<RelTableIDInfo> getRelTableIDInfos() override;

    std::unique_ptr<GraphScanState> prepareScan(common::table_id_t relTableID) override;
    std::unique_ptr<GraphScanState> prepareMultiTableScanFwd(
        std::span<common::table_id_t> nodeTableIDs) override;
    std::unique_ptr<GraphScanState> prepareMultiTableScanBwd(
        std::span<common::table_id_t> nodeTableIDs) override;

    std::vector<common::nodeID_t> scanFwd(common::nodeID_t nodeID, GraphScanState& state) override;
    std::vector<common::nodeID_t> scanFwdRandom(common::nodeID_t nodeID,
        GraphScanState& state) override;
    std::vector<common::nodeID_t> scanBwd(common::nodeID_t nodeID, GraphScanState& state) override;
    std::vector<common::nodeID_t> scanBwdRandom(common::nodeID_t nodeID,
        GraphScanState& state) override;

private:
    void scan(common::nodeID_t nodeID, storage::RelTable* relTable,
        OnDiskGraphScanStates& scanState, storage::RelTableScanState& relTableScanState,
        std::vector<common::nodeID_t>& nbrNodeIDs);

private:
    main::ClientContext* context;
    GraphEntry graphEntry;
    common::table_id_map_t<storage::NodeTable*> nodeIDToNodeTable;
    common::table_id_map_t<common::table_id_map_t<storage::RelTable*>> nodeTableIDToFwdRelTables;
    common::table_id_map_t<common::table_id_map_t<storage::RelTable*>> nodeTableIDToBwdRelTables;
};

} // namespace graph
} // namespace kuzu
