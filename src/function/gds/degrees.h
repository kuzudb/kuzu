#include "function/gds/compute.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/gds_utils.h"
#include "graph/graph.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace function {

using degree_t = uint64_t;
static constexpr degree_t INVALID_DEGREE = UINT64_MAX;

class Degrees {
public:
    Degrees(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        init(numNodesMap, mm);
    }

    void pinTable(table_id_t tableID) { degreeValues = degreeValuesMap.getData(tableID); }

    void addDegree(offset_t offset, uint64_t degree) {
        degreeValues[offset].fetch_add(degree, std::memory_order_relaxed);
    }

    void decreaseDegreeByOne(offset_t offset) {
        degreeValues[offset].fetch_sub(1, std::memory_order_relaxed);
    }

    degree_t getValue(offset_t offset) {
        return degreeValues[offset].load(std::memory_order_relaxed);
    }

private:
    void init(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : numNodesMap) {
            degreeValuesMap.allocate(tableID, numNodes, mm);
            pinTable(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                degreeValues[i].store(0, std::memory_order_relaxed);
            }
        }
    }

private:
    std::atomic<degree_t>* degreeValues = nullptr;
    ObjectArraysMap<std::atomic<degree_t>> degreeValuesMap;
};

struct DegreeEdgeCompute : public EdgeCompute {
    Degrees* degrees;

    explicit DegreeEdgeCompute(Degrees* degrees) : degrees{degrees} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        degrees->addDegree(boundNodeID.offset, chunk.size());
        return {};
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<DegreeEdgeCompute>(degrees);
    }
};

struct DegreesUtils {
    static void computeDegree(processor::ExecutionContext* context, graph::Graph* graph,
        Degrees* degrees, ExtendDirection direction) {
        auto currentFrontier = PathLengths::getFrontier(context, graph, PathLengths::UNVISITED);
        auto nextFrontier = PathLengths::getFrontier(context, graph, 0);
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);
        frontierPair->setActiveNodesForNextIter();
        frontierPair->getNextSparseFrontier().disable();
        auto ec = std::make_unique<DegreeEdgeCompute>(degrees);
        auto computeState = GDSComputeState(std::move(frontierPair), std::move(ec), nullptr);
        GDSUtils::runFrontiersUntilConvergence(context, computeState, graph, direction,
            1 /* maxIters */);
    }
};

} // namespace function
} // namespace kuzu
