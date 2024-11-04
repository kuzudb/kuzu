#include "binder/binder.h"
#include "common/types/internal_id_util.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "function/gds/rec_joins.h"
#include "function/gds/gds_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {
struct WCCOutputs {
public:
    WCCOutputs(common::table_id_map_t<common::offset_t> numNodesMap, storage::MemoryManager* mm) {
        componentIDs = std::make_shared<ComponentIDs>(numNodesMap, mm);
    }

    void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID, common::table_id_t nextFrontierTableID) {
        componentIDs->fixCurFrontierNodeTable(curFrontierTableID);
        componentIDs->fixNextFrontierNodeTable(nextFrontierTableID);
    }

    void beginWritingOutputsForDstNodesInTable(common::table_id_t tableID) {
        componentIDs->fixCurFrontierNodeTable(tableID);
    }

public:
    std::shared_ptr<ComponentIDs> componentIDs;
};

class WCCOutputWriter {
public:
    explicit WCCOutputWriter(main::ClientContext* context, WCCOutputs* wccOutputs, processor::NodeOffsetMaskMap* outputNodeMask) 
    : context{context}, wccOutputs{wccOutputs}, outputNodeMask{outputNodeMask} {
        auto mm = context->getMemoryManager();
        nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        groupVector = std::make_unique<ValueVector>(LogicalType::INT64(), mm);
        nodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        groupVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(nodeIDVector.get());
        vectors.push_back(groupVector.get());
    }

    void materialize(main::ClientContext* context, graph::Graph* graph,
        const common::node_id_map_t<int64_t>& visitedMap, FactorizedTable& table) const {
        for (auto tableID : graph->getNodeTableIDs()) {
            for (auto offset = 0u; offset < graph->getNumNodes(context->getTx(), tableID);
                 ++offset) {
                auto nodeID = nodeID_t{offset, tableID};
                nodeIDVector->setValue<nodeID_t>(0, nodeID);
                groupVector->setValue<int64_t>(0, visitedMap.at(nodeID));
                table.append(vectors);
            }
        }
    }

    void beginWritingForDstNodesInTable(common::table_id_t tableID) {
        wccOutputs->beginWritingOutputsForDstNodesInTable(tableID);
    }

    bool skip(common::nodeID_t dstNodeID) {
        return false;
    }

    void writeComponentID(processor::FactorizedTable& localFT, nodeID_t nodeID) {
        uint16_t componentID = wccOutputs->componentIDs->getMaskValueFromCurFrontierFixedMask(nodeID.offset);

        std::vector<uint64_t> row = {nodeID.offset, componentID};
        localFT.append(vectors);
    }

    std::unique_ptr<WCCOutputWriter> copy() const {
        return std::make_unique<WCCOutputWriter>(*this);
    }

private:
    main::ClientContext* context;
    WCCOutputs* wccOutputs;
    processor::NodeOffsetMaskMap* outputNodeMask;

    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> groupVector;
    std::vector<ValueVector*> vectors;
};

// Edge compute function which for neighbouring nodes, determines whether a node needs to update for
// the next frontier
class WCCEdgeCompute : public EdgeCompute {
public:
    WCCEdgeCompute(WCCFrontierPair* frontierPair) : frontierPair{frontierPair} {}

    void edgeCompute(nodeID_t boundNodeID, std::span<const nodeID_t> nbrNodeIDs,
        std::span<const relID_t> edgeIDs, SelectionVector& mask, bool isFwd) override {
        size_t activeCount = 0;
        auto boundComponentID =
            frontierPair->componentIDs->getMaskValueFromCurFrontierFixedMask(boundNodeID.offset);
        // If the neighbouring node's componentID is larger, it needs to be updated.
        mask.forEach([&](auto i) {
            auto nbrComponentID = frontierPair->componentIDs->getMaskValueFromCurFrontierFixedMask(
                nbrNodeIDs[i].offset);
            if (nbrComponentID > boundComponentID) {
                frontierPair->componentIDs->updateComponentID(nbrNodeIDs[i], boundComponentID);
                mask.getMutableBuffer()[activeCount++] = i;
            }
        });
        mask.setToFiltered(activeCount);
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WCCEdgeCompute>(frontierPair);
    }

private:
    WCCFrontierPair* frontierPair;
};

class WCCVertexCompute : public VertexCompute {
public:
    explicit WCCVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState, std::unique_ptr<WCCOutputWriter> writer)
    : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~WCCVertexCompute() override { sharedState->returnLocalTable(localFT);}
    void beginOnTable(table_id_t tableID) override {
        writer->beginWritingForDstNodesInTable(tableID);
    }

    void vertexCompute(nodeID_t nodeID) override {
        if (writer->skip(nodeID)) {
            return;
        }
    }

    void finalizeWorkerThread() override {
        // Do nothing.
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WCCVertexCompute>(mm, sharedState, writer->copy());
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    processor::FactorizedTable* localFT;
    std::unique_ptr<WCCOutputWriter> writer;
};

struct WCCCompState {
    std::unique_ptr<WCCFrontierPair> frontierPair;
    std::unique_ptr<WCCEdgeCompute> edgeCompute;
    std::unique_ptr<WCCOutputs> outputs;
    std::unique_ptr<WCCOutputWriter> outputWriter;

    WCCCompState(std::unique_ptr<WCCFrontierPair> frontierPair,
                std::unique_ptr<WCCEdgeCompute> edgeCompute,
                std::unique_ptr<WCCOutputs> outputs,
                std::unique_ptr<WCCOutputWriter> outputWriter) :
                frontierPair{std::move(frontierPair)},
                edgeCompute{std::move(edgeCompute)},
                outputs{std::move(outputs)},
                outputWriter{std::move(outputWriter)} {}

    void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID, common::table_id_t nextFrontierTableID) const {
        frontierPair->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
        outputs->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
    }
};

struct WCCBindData final : public GDSBindData {
    std::shared_ptr<binder::Expression> nodeInput;

    bool writeComponentIDs = true;

    WCCBindData(std::shared_ptr<binder::Expression> nodeInput,
                std::shared_ptr<binder::Expression> nodeOutput)
    : GDSBindData{std::move(nodeOutput)}, nodeInput{std::move(nodeInput)} {}

    WCCBindData(const WCCBindData& other) :
        GDSBindData(other),
        nodeInput{other.nodeInput},
        writeComponentIDs{other.writeComponentIDs} {}

    bool hasNodeInput() const override {return true;}

    std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }

    bool hasNodeOutput() const override { return true; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<WCCBindData>(*this);
    }
};

class WeaklyConnectedComponent final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

public:
    WeaklyConnectedComponent() = default;
    WeaklyConnectedComponent(const WeaklyConnectedComponent& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * graph::ANY
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return std::vector<LogicalTypeID>{LogicalTypeID::ANY};
    }

    /*
     * Outputs are
     *
     * _node._id::INTERNAL_ID
     * group_id::INT64
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
        return columns;
    }

    void bind(const expression_vector&, Binder* binder, GraphEntry& graphEntry) override {
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        bindData = std::make_unique<GDSBindData>(nodeOutput);
    }

    void initLocalState(main::ClientContext* context) override {
        return;
    }

    void exec(processor::ExecutionContext* executionContext) override {
        auto clientContext = executionContext->clientContext;
        auto inputNodeMaskMap = sharedState->getInputNodeMaskMap();

        for (auto& tableID : sharedState->graph->getNodeTableIDs()) {
            if (!inputNodeMaskMap->containsTableID(tableID)) {
                continue;
            }
            auto calcFun = [tableID, executionContext, clientContext, this](offset_t offset) {
                auto sourceNodeID = nodeID_t{offset, tableID};
                WCCCompState wccCompState = getWCCCompState(executionContext, sourceNodeID);
                auto wccBindData = bindData->ptrCast<WCCBindData>();

                GDSUtils::runWCCFrontiersUntilConvergence(executionContext, wccCompState, sharedState->graph.get(), 10);
            
                auto vertexCompute = std::make_unique<WCCVertexCompute>(clientContext->getMemoryManager(),
                                                                        sharedState.get(),
                                                                        wccCompState.outputWriter->copy());
                GDSUtils::runVertexComputeIteration(executionContext, sharedState->graph.get(), *vertexCompute);
            };

            auto numNodes = sharedState->graph->getNumNodes(clientContext->getTx(), tableID);
            auto mask = inputNodeMaskMap->getOffsetMask(tableID);

            if (mask->isEnabled()) {
                for (const auto& offset : mask->range(0, numNodes)) {
                    calcFun(offset);
                }
            } else {
                for (auto offset = 0u; offset < numNodes; ++offset) {
                    calcFun(offset);
                }
            }
        }

        sharedState->mergeLocalTables();
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<WeaklyConnectedComponent>(*this);
    }

private:
    // void findConnectedComponent(common::nodeID_t nodeID, int64_t groupID,
    //     GraphScanState& scanState) {
    //     KU_ASSERT(!visitedMap.contains(nodeID));
    //     visitedMap.insert({nodeID, groupID});
    //     // Collect the nodes so that the recursive scan doesn't begin until this scan is done
    //     for (auto& nbr : sharedState->graph->scanFwd(nodeID, scanState).collectNbrNodes()) {
    //         if (!visitedMap.contains(nbr)) {
    //             findConnectedComponent(nbr, groupID, scanState);
    //         }
    //     }
    // }

    WCCCompState getWCCCompState(ExecutionContext* context, nodeID_t sourceNodeID) {
        auto clientContext = context->clientContext;
        auto output = std::make_unique<WCCOutputs>(
            sharedState->graph->getNumNodesMap(clientContext->getTx()),
            clientContext->getMemoryManager());
        auto outputWriter = std::make_unique<WCCOutputWriter>(clientContext, output.get(), sharedState->getOutputNodeMaskMap());
        auto frontierPair = std::make_unique<WCCFrontierPair>(output->componentIDs, clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<WCCEdgeCompute>(frontierPair.get());
        return WCCCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output), std::move(outputWriter));
    }

private:
    common::node_id_map_t<int64_t> visitedMap;
    std::unique_ptr<WCCOutputWriter> localState;
};

function_set WeaklyConnectedComponentsFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<WeaklyConnectedComponent>();
    auto function =
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo));
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
