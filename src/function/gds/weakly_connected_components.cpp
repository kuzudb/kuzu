#include "binder/binder.h"
#include "common/types/internal_id_util.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

class WeaklyConnectedComponentOutputWriter {
public:
    explicit WeaklyConnectedComponentOutputWriter(main::ClientContext* context) {
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

private:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> groupVector;
    std::vector<ValueVector*> vectors;
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
        localState = std::make_unique<WeaklyConnectedComponentOutputWriter>(context);
    }

    void exec(processor::ExecutionContext* context) override {
        auto graph = sharedState->graph.get();
        localState = std::make_unique<WeaklyConnectedComponentOutputWriter>(context->clientContext);
        visitedMap.clear();
        auto groupID = 0;
        auto nodeTableIDs = graph->getNodeTableIDs();
        auto scanState = graph->prepareMultiTableScanFwd(nodeTableIDs);
        for (auto tableID : nodeTableIDs) {
            for (auto offset = 0u;
                 offset < graph->getNumNodes(context->clientContext->getTx(), tableID); ++offset) {
                auto nodeID = nodeID_t{offset, tableID};
                if (visitedMap.contains(nodeID)) {
                    continue;
                }
                findConnectedComponent(nodeID, groupID++, *scanState);
            }
        }
        localState->materialize(context->clientContext, graph, visitedMap, *sharedState->fTable);
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<WeaklyConnectedComponent>(*this);
    }

private:
    void findConnectedComponent(common::nodeID_t nodeID, int64_t groupID,
        GraphScanState& scanState) {
        KU_ASSERT(!visitedMap.contains(nodeID));
        visitedMap.insert({nodeID, groupID});
        // Collect the nodes so that the recursive scan doesn't begin until this scan is done
        for (auto& nbr : sharedState->graph->scanFwd(nodeID, scanState).collectNbrNodes()) {
            if (!visitedMap.contains(nbr)) {
                findConnectedComponent(nbr, groupID, scanState);
            }
        }
    }

private:
    common::node_id_map_t<int64_t> visitedMap;
    std::unique_ptr<WeaklyConnectedComponentOutputWriter> localState;
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
