#pragma once

#include "common/mask.h"
#include "graph/graph.h"
#include "processor/result/factorized_table_pool.h"

namespace kuzu {
namespace processor {

struct KUZU_API GDSCallSharedState {
    std::unique_ptr<graph::Graph> graph;

    GDSCallSharedState(std::shared_ptr<FactorizedTable> fTable, std::unique_ptr<graph::Graph> graph)
        : graph{std::move(graph)}, factorizedTablePool{std::move(fTable)} {}
    DELETE_COPY_AND_MOVE(GDSCallSharedState);

    void setGraphNodeMask(std::unique_ptr<common::NodeOffsetMaskMap> maskMap);
    common::NodeOffsetMaskMap* getGraphNodeMaskMap() const { return graphNodeMask.get(); }

public:
    FactorizedTablePool factorizedTablePool;

private:
    std::unique_ptr<common::NodeOffsetMaskMap> graphNodeMask = nullptr;
};

} // namespace processor
} // namespace kuzu
