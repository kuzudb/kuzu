#include "function/gds/bfs_graph.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct SingleSPOutputs : public SPOutputs {
    SingleSPOutputs(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        common::nodeID_t sourceNodeID, MemoryManager* mm = nullptr)
        : SPOutputs(nodeTableIDAndNumNodes, sourceNodeID, mm) {}
    // Note: We do not fix the node table for pathLengths, because PathLengths is a
    // FrontierPair implementation and RJCompState will call beginFrontierComputeBetweenTables
    // on FrontierPair (and RJOutputs). That's why this function is empty.
    void beginFrontierComputeBetweenTables(table_id_t, table_id_t) override{};

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->fixCurFrontierNodeTable(tableID);
    }
};

class SingleSPDestinationsLengthOutputWriter : public DestinationsOutputWriter {
public:
    SingleSPDestinationsLengthOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs)
        : DestinationsOutputWriter(context, rjOutputs) {
        lengthVector =
            std::make_unique<ValueVector>(LogicalType::INT64(), context->getMemoryManager());
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(lengthVector.get());
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SingleSPDestinationsLengthOutputWriter>(context, rjOutputs);
    }

protected:
    void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t,
        uint16_t length) const override {
        lengthVector->setValue<int64_t>(0, length);
        fTable.append(vectors);
    }

protected:
    std::unique_ptr<common::ValueVector> lengthVector;
};

class SingleSPLengthsEdgeCompute : public EdgeCompute {
public:
    explicit SingleSPLengthsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair)
        : frontierPair{frontierPair} {};

    bool edgeCompute(common::nodeID_t, common::nodeID_t nbrID, relID_t) override {
        return frontierPair->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrID.offset) ==
               PathLengths::UNVISITED;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SingleSPLengthsEdgeCompute>(frontierPair);
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

    bool edgeCompute(nodeID_t boundNodeID, nodeID_t nbrNodeID, relID_t edgeID) override {
        auto shouldUpdate = frontierPair->pathLengths->getMaskValueFromNextFrontierFixedMask(
                                nbrNodeID.offset) == PathLengths::UNVISITED;
        if (shouldUpdate) {
            if (!parentListBlock->hasSpace()) {
                parentListBlock = bfsGraph->addNewBlock();
            }
            bfsGraph->tryAddSingleParent(frontierPair->curIter.load(std::memory_order_relaxed),
                parentListBlock, nbrNodeID /* child */, boundNodeID /* parent */, edgeID);
        }
        return shouldUpdate;
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

    expression_vector getResultColumns(Binder*) const override { return getNodeIDResultColumns(); }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPDestinationsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output =
            std::make_unique<SingleSPOutputs>(sharedState->graph->getNodeTableIDAndNumNodes(),
                sourceNodeID, clientContext->getMemoryManager());
        auto outputWriter = std::make_unique<DestinationsOutputWriter>(clientContext, output.get());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<SingleSPLengthsEdgeCompute>(frontierPair.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

class SingleSPLengthsAlgorithm : public SPAlgorithm {
public:
    SingleSPLengthsAlgorithm() = default;
    SingleSPLengthsAlgorithm(const SingleSPLengthsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder* binder) const override {
        auto columns = getNodeIDResultColumns();
        columns.push_back(getLengthColumn(binder));
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPLengthsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output =
            std::make_unique<SingleSPOutputs>(sharedState->graph->getNodeTableIDAndNumNodes(),
                sourceNodeID, clientContext->getMemoryManager());
        auto outputWriter =
            std::make_unique<SingleSPDestinationsLengthOutputWriter>(clientContext, output.get());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<SingleSPLengthsEdgeCompute>(frontierPair.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

class SingleSPPathsAlgorithm : public SPAlgorithm {
public:
    SingleSPPathsAlgorithm() = default;
    SingleSPPathsAlgorithm(const SingleSPPathsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder* binder) const override {
        auto columns = getNodeIDResultColumns();
        columns.push_back(getLengthColumn(binder));
        columns.push_back(getPathNodeIDsColumn(binder));
        columns.push_back(getPathEdgeIDsColumn(binder));
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output =
            std::make_unique<PathsOutputs>(sharedState->graph->getNodeTableIDAndNumNodes(),
                sourceNodeID, clientContext->getMemoryManager());
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto outputWriter = std::make_unique<SPPathsOutputWriter>(clientContext, output.get(),
            rjBindData->upperBound);
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
    result.push_back(
        std::make_unique<GDSFunction>(name, std::make_unique<SingleSPDestinationsAlgorithm>()));
    return result;
}

function_set SingleSPLengthsFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<GDSFunction>(name, std::make_unique<SingleSPLengthsAlgorithm>()));
    return result;
}

function_set SingleSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<GDSFunction>(name, std::make_unique<SingleSPPathsAlgorithm>()));
    return result;
}

} // namespace function
} // namespace kuzu
