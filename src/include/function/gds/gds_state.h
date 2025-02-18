#pragma once

#include "auxiliary_state/gds_auxilary_state.h"
#include "gds_frontier.h"

namespace kuzu {
namespace function {

struct KUZU_API GDSComputeState {
    std::unique_ptr<function::FrontierPair> frontierPair = nullptr;
    std::unique_ptr<function::EdgeCompute> edgeCompute = nullptr;
    std::unique_ptr<function::GDSAuxiliaryState> auxiliaryState = nullptr;
    // While stepActiveRelTableIDs is empty, using all relTableIDs in graph
    std::vector<common::table_id_set_t> stepActiveRelTableIDs;

    processor::NodeOffsetMaskMap* outputNodeMask = nullptr;

    GDSComputeState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute,
        std::unique_ptr<function::GDSAuxiliaryState> auxiliaryState,
        std::vector<common::table_id_set_t> stepActiveRelTableIDs,
        processor::NodeOffsetMaskMap* outputNodeMask)
        : frontierPair{std::move(frontierPair)}, edgeCompute{std::move(edgeCompute)},
          auxiliaryState{std::move(auxiliaryState)},
          stepActiveRelTableIDs{std::move(stepActiveRelTableIDs)}, outputNodeMask{outputNodeMask} {}

    void initSource(common::nodeID_t sourceNodeID) const;
    // When performing computations on multi-label graphs, it is beneficial to fix a single
    // node table of nodes in the current frontier and a single node table of nodes for the next
    // frontier. That is because algorithms will perform extensions using a single relationship
    // table at a time, and each relationship table R is between a single source node table S and
    // a single destination node table T. Therefore, during execution the algorithm will need to
    // check only the active S nodes in current frontier and update the active statuses of only the
    // T nodes in the next frontier. The information that the algorithm is beginning and S-to-T
    // extensions are be given to the data structures of the computation, e.g., FrontierPairs and
    // RJOutputs, to possibly avoid them doing lookups of S and T-related data structures,
    // e.g., maps, internally.
    void beginFrontierCompute(common::table_id_t currTableID, common::table_id_t nextTableID) const;

    common::table_id_set_t getActiveRelTableIDs(size_t index, graph::Graph* graph);
};

} // namespace function
} // namespace kuzu
