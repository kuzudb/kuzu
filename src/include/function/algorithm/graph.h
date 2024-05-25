#pragma once

#include "common/enums/rel_direction.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace main {
class ClientContext;
}

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

class Graph {
public:
    explicit Graph(main::ClientContext* context) : context{context} {}
    virtual ~Graph() = default;

    virtual common::offset_t getNumNodes() = 0;

    virtual common::offset_t getNumEdges() = 0;

    virtual uint64_t getFwdDegreeOffset(common::offset_t offset) = 0;

    virtual void initializeStateFwdNbrs(common::offset_t offset, NbrScanState *nbrScanState) = 0;

    virtual bool hasMoreFwdNbrs(NbrScanState *nbrScanState) = 0;

    virtual common::ValueVector* getFwdNbrs(NbrScanState *nbrScanState) = 0;

protected:
    main::ClientContext* context;
};

} // namespace graph
} // namespace kuzu
