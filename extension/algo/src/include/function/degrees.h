#pragma once

#include "function/gds/compute.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/gds_utils.h"
#include "graph/graph.h"

namespace kuzu {
namespace algo_extension {

using degree_t = uint64_t;
static constexpr degree_t INVALID_DEGREE = UINT64_MAX;

class Degrees {
public:
    Degrees(const common::table_id_map_t<common::offset_t>& maxOffsetMap,
        storage::MemoryManager* mm) {
        for (const auto& [tableID, maxOffset] : maxOffsetMap) {
            degreeValuesMap.allocate(tableID, maxOffset, mm);
            pinTable(tableID);
            for (auto i = 0u; i < maxOffset; ++i) {
                degreeValues[i].store(0, std::memory_order_relaxed);
            }
        }
    }

    void pinTable(common::table_id_t tableID) { degreeValues = degreeValuesMap.getData(tableID); }

    void addDegree(common::offset_t offset, uint64_t degree) {
        degreeValues[offset].fetch_add(degree, std::memory_order_relaxed);
    }

    void decreaseDegreeByOne(common::offset_t offset) {
        degreeValues[offset].fetch_sub(1, std::memory_order_relaxed);
    }

    degree_t getValue(common::offset_t offset) {
        return degreeValues[offset].load(std::memory_order_relaxed);
    }

private:
    std::atomic<degree_t>* degreeValues = nullptr;
    function::GDSDenseObjectManager<std::atomic<degree_t>> degreeValuesMap;
};

struct DegreeEdgeCompute : public function::EdgeCompute {
    Degrees* degrees;

    explicit DegreeEdgeCompute(Degrees* degrees) : degrees{degrees} {}

    std::vector<common::nodeID_t> edgeCompute(common::nodeID_t boundNodeID,
        graph::NbrScanState::Chunk& chunk, bool) override {
        degrees->addDegree(boundNodeID.offset, chunk.size());
        return {};
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<DegreeEdgeCompute>(degrees);
    }
};

class DegreesGDSAuxiliaryState : public function::GDSAuxiliaryState {
public:
    explicit DegreesGDSAuxiliaryState(Degrees* degrees) : degrees{degrees} {};

    void beginFrontierCompute(common::table_id_t curTableID, common::table_id_t) override {
        degrees->pinTable(curTableID);
    }

    void switchToDense(processor::ExecutionContext*, graph::Graph*) override {}

private:
    Degrees* degrees;
};

struct DegreesUtils {
    static void computeDegree(processor::ExecutionContext* context, graph::Graph* graph,
        common::NodeOffsetMaskMap* nodeOffsetMaskMap, Degrees* degrees,
        common::ExtendDirection direction) {
        auto currentFrontier = function::DenseFrontier::getUnvisitedFrontier(context, graph);
        auto nextFrontier =
            function::DenseFrontier::getVisitedFrontier(context, graph, nodeOffsetMaskMap);
        auto frontierPair = std::make_unique<function::DenseFrontierPair>(
            std::move(currentFrontier), std::move(nextFrontier));
        frontierPair->setActiveNodesForNextIter();
        auto ec = std::make_unique<DegreeEdgeCompute>(degrees);
        auto auxiliaryState = std::make_unique<DegreesGDSAuxiliaryState>(degrees);
        auto computeState = function::GDSComputeState(std::move(frontierPair), std::move(ec),
            std::move(auxiliaryState));
        function::GDSUtils::runAlgorithmEdgeCompute(context, computeState, graph, direction,
            1 /* maxIters */);
    }
};

} // namespace algo_extension
} // namespace kuzu
