#pragma once

#include "common/mask.h"
#include "graph/graph.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct GDSCallSharedState {
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> fTable;
    std::unique_ptr<graph::Graph> graph;
    common::table_id_map_t<std::unique_ptr<common::NodeOffsetLevelSemiMask>> inputNodeOffsetMasks;

    GDSCallSharedState(std::shared_ptr<FactorizedTable> fTable, std::unique_ptr<graph::Graph> graph,
        common::table_id_map_t<std::unique_ptr<common::NodeOffsetLevelSemiMask>>
            inputNodeOffsetMasks)
        : fTable{std::move(fTable)}, graph{std::move(graph)},
          inputNodeOffsetMasks{std::move(inputNodeOffsetMasks)} {}
    DELETE_COPY_AND_MOVE(GDSCallSharedState);
};

} // namespace processor
} // namespace kuzu
