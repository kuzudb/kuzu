#include "common/data_chunk/sel_vector.h"
#include "function/gds/bfs_graph.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "processor/execution_context.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct SingleSPDestinationsOutputs : public SPOutputs {
    SingleSPDestinationsOutputs(common::table_id_map_t<common::offset_t> numNodesMap,
        common::nodeID_t sourceNodeID, MemoryManager* mm = nullptr)
        : SPOutputs(numNodesMap, sourceNodeID, mm) {}
    // Note: We do not fix the node table for pathLengths, because PathLengths is a
    // FrontierPair implementation and RJCompState will call beginFrontierComputeBetweenTables
    // on FrontierPair (and RJOutputs). That's why this function is empty.
    void beginFrontierComputeBetweenTables(table_id_t, table_id_t) override{};

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->fixCurFrontierNodeTable(tableID);
    }
};

class SingleSPDestinationsEdgeCompute : public EdgeCompute {
public:
    explicit SingleSPDestinationsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair)
        : frontierPair{frontierPair} {};

    void edgeCompute(common::nodeID_t, std::span<const common::nodeID_t> nbrIDs,
        std::span<const relID_t>, SelectionVector& mask, bool) override {
        size_t activeCount = 0;
        mask.forEach([&](auto i) {
            if (frontierPair->pathLengths->getMaskValueFromNextFrontierFixedMask(
                    nbrIDs[i].offset) == PathLengths::UNVISITED) {
                mask.getMutableBuffer()[activeCount++] = i;
            }
        });
        mask.setToFiltered(activeCount);
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SingleSPDestinationsEdgeCompute>(frontierPair);
    }

private:
    SinglePathLengthsFrontierPair* frontierPair;
};

class SingleSPPathsEdgeCompute : public EdgeCompute {
public:
    SingleSPPathsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair, BFSGraph* bfsGraph)
        : frontierPair{frontierPair}, bfsGraph{bfsGraph} {
        parentListBlock = bfsGraph->addNewBlock();
    }

    void edgeCompute(nodeID_t boundNodeID, std::span<const nodeID_t> nbrNodeIDs,
        std::span<const relID_t> edgeIDs, SelectionVector& mask, bool isFwd) override {
        size_t activeCount = 0;
        mask.forEach([&](auto i) {
            auto shouldUpdate = frontierPair->pathLengths->getMaskValueFromNextFrontierFixedMask(
                                    nbrNodeIDs[i].offset) == PathLengths::UNVISITED;
            if (shouldUpdate) {
                if (!parentListBlock->hasSpace()) {
                    parentListBlock = bfsGraph->addNewBlock();
                }
                bfsGraph->tryAddSingleParent(frontierPair->curIter.load(std::memory_order_relaxed),
                    parentListBlock, nbrNodeIDs[i] /* child */, boundNodeID /* parent */,
                    edgeIDs[i], isFwd);
                mask.getMutableBuffer()[activeCount++] = i;
            }
        });
        mask.setToFiltered(activeCount);
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SingleSPPathsEdgeCompute>(frontierPair, bfsGraph);
    }

private:
    SinglePathLengthsFrontierPair* frontierPair;
    BFSGraph* bfsGraph;
    ObjectBlock<ParentList>* parentListBlock = nullptr;
};

/**
 * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
 * one arbitrary shortest path is returned for each destination. If paths are not returned,
 * multiplicities of each destination is ignored (e.g., if there are 3 paths to a destination d,
 * d is returned only once).
 */
class SingleSPDestinationsAlgorithm : public SPAlgorithm {
public:
    SingleSPDestinationsAlgorithm() = default;
    SingleSPDestinationsAlgorithm(const SingleSPDestinationsAlgorithm& other)
        : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder*) const override { return getBaseResultColumns(); }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPDestinationsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output = std::make_unique<SingleSPDestinationsOutputs>(
            sharedState->graph->getNumNodesMap(), sourceNodeID, clientContext->getMemoryManager());
        auto outputWriter = std::make_unique<DestinationsOutputWriter>(clientContext, output.get());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<SingleSPDestinationsEdgeCompute>(frontierPair.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

class SingleSPPathsAlgorithm : public SPAlgorithm {
public:
    SingleSPPathsAlgorithm() = default;
    SingleSPPathsAlgorithm(const SingleSPPathsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder*) const override {
        auto columns = getBaseResultColumns();
        auto rjBindData = bindData->ptrCast<RJBindData>();
        columns.push_back(rjBindData->pathNodeIDsExpr);
        columns.push_back(rjBindData->pathEdgeIDsExpr);
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output = std::make_unique<PathsOutputs>(sharedState->graph->getNumNodesMap(),
            sourceNodeID, clientContext->getMemoryManager());
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<SPPathsOutputWriter>(clientContext, output.get(),
            std::move(writerInfo));
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute =
            std::make_unique<SingleSPPathsEdgeCompute>(frontierPair.get(), &output->bfsGraph);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

function_set SingleSPDestinationsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction SingleSPDestinationsFunction::getFunction() {
    auto algo = std::make_unique<SingleSPDestinationsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

function_set SingleSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction SingleSPPathsFunction::getFunction() {
    auto algo = std::make_unique<SingleSPPathsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

} // namespace function
} // namespace kuzu
