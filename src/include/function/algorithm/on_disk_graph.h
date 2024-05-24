#pragma once

#include "graph.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace graph {

struct NbrScanState {
    std::shared_ptr<common::DataChunkState> srcNodeIDVectorState;
    std::shared_ptr<common::DataChunkState> dstNodeIDVectorState;
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::vector<common::ValueVector*> outputNodeIDVectors;

    static constexpr common::RelDataDirection direction = common::RelDataDirection::FWD;
    std::vector<common::column_id_t> columnIDs;
    std::unique_ptr<storage::RelTableReadState> fwdReadState;

    explicit NbrScanState(storage::MemoryManager* mm);
};

class OnDiskGraph : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, const std::string& nodeTableName,
        const std::string& relTableName);

    common::offset_t getNumNodes() override;

    common::offset_t getNumEdges() override;

    uint64_t getFwdDegreeOffset(common::offset_t offset) override;

    common::ValueVector* getFwdNbrs(common::offset_t offset) override;

private:
    storage::NodeTable* nodeTable;
    storage::RelTable* relTable;
    std::unique_ptr<NbrScanState> nbrScanState;
};

} // namespace graph
} // namespace kuzu
