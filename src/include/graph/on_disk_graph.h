#pragma once

#include "graph.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace graph {

struct NbrScanState {
    std::shared_ptr<common::DataChunkState> srcNodeIDVectorState;
    std::shared_ptr<common::DataChunkState> dstNodeIDVectorState;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;

    static constexpr common::RelDataDirection direction = common::RelDataDirection::FWD;
    std::vector<common::column_id_t> columnIDs;
    std::unique_ptr<storage::RelTableScanState> fwdReadState;

    explicit NbrScanState(storage::MemoryManager* mm);
};

class OnDiskGraph : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, common::table_id_t nodeTableID,
        common::table_id_t relTableID);

    common::table_id_t getNodeTableID() override { return nodeTable->getTableID(); }

    common::offset_t getNumNodes() override;

    common::offset_t getNumEdges() override;

    std::vector<common::nodeID_t> getNbrs(common::offset_t offset) override;

private:
    main::ClientContext* context;
    storage::NodeTable* nodeTable;
    storage::RelTable* relTable;
    std::unique_ptr<NbrScanState> nbrScanState;
};

} // namespace graph
} // namespace kuzu
