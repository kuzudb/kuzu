#include <stack>

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/gds_utils.h"
#include "function/gds/output_writer.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

using namespace std;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

// Use the two largest offset_t values as special boolean values instead of
// using a separate second array.
static constexpr offset_t NOT_VISITED = numeric_limits<offset_t>::max();
static constexpr offset_t VISITED = numeric_limits<offset_t>::max() - 1u;

class SCCState {
public:
    SCCState(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        if (numNodesMap.size() != 1) {
            throw BinderException("SCC only supports operations on one node table.");
        }
        for (const auto& [tableID, numNodes] : numNodesMap) {
            this->numNodes = numNodes;
            this->tableID = tableID;
            componentIDsMap.allocate(tableID, numNodes, mm);
            componentIDs = componentIDsMap.getData(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                componentIDs[i] = NOT_VISITED;
            }
        }
    }

    void setVisited(const offset_t offset) const { componentIDs[offset] = VISITED; }

    bool visited(const offset_t offset) const { return componentIDs[offset] == VISITED; }

    bool componentIDSet(const offset_t offset) const { return componentIDs[offset] < VISITED; }

    void setComponentID(const offset_t offset, const offset_t value) const {
        componentIDs[offset] = value;
    }

    offset_t getComponentID(const offset_t offset) const { return componentIDs[offset]; }

    offset_t getNumNodes() const { return numNodes; }

    table_id_t getTableID() const { return tableID; }

private:
    offset_t* componentIDs = nullptr;
    ObjectArraysMap<offset_t> componentIDsMap;

    offset_t numNodes;
    table_id_t tableID;
};

class SCCCompute {
public:
    SCCCompute(Graph* graph, SCCState& sccState) : graph{graph}, sccState{sccState} {}

    void compute() {
        auto nbrTables = graph->getForwardNbrTableInfos(sccState.getTableID());
        if (nbrTables.size() != 1) {
            throw BinderException("SCC only supports operations on one edge table.");
        }
        auto nbrInfo = nbrTables[0];
        auto relEntry = nbrInfo.relEntry;
        auto scanState = graph->prepareRelScan(relEntry, "");
        for (auto i = 0u; i < sccState.getNumNodes(); ++i) {
            if (!sccState.visited(i)) {
                forwardDFS(i, *scanState);
            }
        }
        while (!dfsStack.empty()) {
            auto node = dfsStack.top();
            dfsStack.pop();
            if (!sccState.componentIDSet(node)) {
                backwardsDFS(node, node, *scanState);
            }
        }
    }

    void forwardDFS(offset_t node, NbrScanState& scanState) {
        toProcess.push(node);

        while (!toProcess.empty()) {
            auto nextNode = toProcess.top();

            if (sccState.visited(nextNode)) {
                toProcess.pop();
                dfsStack.push(nextNode);
                continue;
            }

            sccState.setVisited(nextNode);

            auto nextNodeID = nodeID_t{nextNode, sccState.getTableID()};
            for (auto chunk : graph->scanFwd(nextNodeID, scanState)) {
                chunk.forEach([&](auto nbrNodeID, auto) {
                    if (!sccState.visited(nbrNodeID.offset)) {
                        toProcess.push(nbrNodeID.offset);
                    }
                });
            }
        }
    }

    void backwardsDFS(offset_t node, const offset_t root, NbrScanState& scanState) {
        toProcess.push(node);

        while (!toProcess.empty()) {
            auto nextNode = toProcess.top();
            toProcess.pop();

            if (!sccState.componentIDSet(nextNode)) {
                sccState.setComponentID(nextNode, root);
            }

            auto nextNodeID = nodeID_t{nextNode, sccState.getTableID()};
            for (auto chunk : graph->scanBwd(nextNodeID, scanState)) {
                chunk.forEach([&](auto nbrNodeID, auto) {
                    if (!sccState.componentIDSet(nbrNodeID.offset)) {
                        toProcess.push(nbrNodeID.offset);
                    }
                });
            }
        }
    }

private:
    Graph* graph;
    SCCState& sccState;
    stack<offset_t> toProcess;
};

class SCCOutputWriter : public GDSOutputWriter {
public:
    explicit SCCOutputWriter(main::ClientContext* context) : GDSOutputWriter{context} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID(), context->getMemoryManager());
        componentIDVector = createVector(LogicalType::UINT64(), context->getMemoryManager());
    }

    unique_ptr<SCCOutputWriter> copy() const { return make_unique<SCCOutputWriter>(context); }

public:
    unique_ptr<ValueVector> nodeIDVector;
    unique_ptr<ValueVector> componentIDVector;
};

class SCCVertexCompute : public VertexCompute {
public:
    SCCVertexCompute(MemoryManager* mm, GDSCallSharedState* sharedState,
        unique_ptr<SCCOutputWriter> writer, SCCState& sccState)
        : mm{mm}, sharedState{sharedState}, writer{move(writer)}, sccState{sccState} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~SCCVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(table_id_t /*tableID*/) override { return true; }

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            writer->nodeIDVector->setValue<nodeID_t>(0, nodeID);
            writer->componentIDVector->setValue<uint64_t>(0, sccState.getComponentID(i));
            localFT->append(writer->vectors);
        }
    }

    unique_ptr<VertexCompute> copy() override {
        return make_unique<SCCVertexCompute>(mm, sharedState, writer->copy(), sccState);
    }

private:
    MemoryManager* mm;
    GDSCallSharedState* sharedState;
    unique_ptr<SCCOutputWriter> writer;
    SCCState& sccState;

    FactorizedTable* localFT;
};

class StronglyConnectedComponent final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

public:
    StronglyConnectedComponent() = default;
    StronglyConnectedComponent(const StronglyConnectedComponent& other) : GDSAlgorithm{other} {}

    unique_ptr<GDSAlgorithm> copy() const override {
        return make_unique<StronglyConnectedComponent>(*this);
    }

    vector<LogicalTypeID> getParameterTypeIDs() const override {
        return vector<LogicalTypeID>{LogicalTypeID::ANY};
    }

    expression_vector getResultColumns(const GDSBindInput& bindInput) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(
            bindInput.binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
        return columns;
    }

    void bind(const GDSBindInput& input, main::ClientContext& context) override {
        auto graphName = ExpressionUtil::getLiteralValue<string>(*input.getParam(0));
        auto graphEntry = bindGraphEntry(context, graphName);
        auto nodeOutput = bindNodeOutput(input, graphEntry.getNodeEntries());
        bindData = make_unique<GDSBindData>(move(graphEntry), nodeOutput);
    }

    void exec(ExecutionContext* context) override {
        auto clientContext = context->clientContext;
        auto mm = clientContext->getMemoryManager();
        auto graph = sharedState->graph.get();
        auto numNodesMap = graph->getNumNodesMap(clientContext->getTransaction());

        auto sccState = SCCState(numNodesMap, mm);

        auto edgeCompute = make_unique<SCCCompute>(graph, sccState);
        edgeCompute->compute();

        auto writer = make_unique<SCCOutputWriter>(clientContext);
        auto vertexCompute =
            make_unique<SCCVertexCompute>(mm, sharedState.get(), move(writer), sccState);
        GDSUtils::runVertexCompute(context, graph, *vertexCompute);

        sharedState->mergeLocalTables();
    }
};

function_set StronglyConnectedComponentsFunction::getFunctionSet() {
    function_set result;
    auto algo = make_unique<StronglyConnectedComponent>();
    auto function = make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), move(algo));
    result.push_back(move(function));
    return result;
}

} // namespace function
} // namespace kuzu
