#pragma once

#include "common/enums/rel_direction.h"
#include "common/types/internal_id_t.h"
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

class Graph {
public:
    Graph() = default;
    virtual ~Graph() = default;

    virtual common::table_id_t getNodeTableID() = 0;

    virtual common::offset_t getNumNodes() = 0;

    virtual common::offset_t getNumEdges() = 0;

    virtual std::vector<common::nodeID_t> getNbrs(common::offset_t offset,
        NbrScanState* nbrScanState) = 0;

    virtual void initializeStateFwdNbrs(common::offset_t offset, NbrScanState* nbrScanState) = 0;

    virtual bool hasMoreFwdNbrs(NbrScanState* nbrScanState) = 0;

    virtual void getFwdNbrs(NbrScanState* nbrScanState) = 0;

public:
    bool isInMemory;
};

} // namespace graph
} // namespace kuzu
