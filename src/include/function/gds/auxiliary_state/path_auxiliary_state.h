#pragma once

#include "function/gds/bfs_graph.h"
#include "gds_auxilary_state.h"

namespace kuzu {
namespace function {

class PathAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit PathAuxiliaryState(std::unique_ptr<BFSGraph> bfsGraph)
        : bfsGraph{std::move(bfsGraph)} {}

    void beginFrontierCompute(common::table_id_t, common::table_id_t toTableID) override {
        bfsGraph->pinTableID(toTableID);
    }

private:
    std::unique_ptr<BFSGraph> bfsGraph;
};

} // namespace function
} // namespace kuzu
