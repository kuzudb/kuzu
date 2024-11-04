#include "function/gds/rec_joins.h"
#include "binder/binder.h"
#include "common/types/internal_id_util.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
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
    explicit WCCOutputWriter(main::ClientContext* context) {
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

class WCCAlgorithm final : public GDSAlgorithm {
    static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

public:
    WCCAlgorithm() = default;
    WCCAlgorithm(const WCCAlgorithm& other) : GDSAlgorithm{other} {}

    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return std::vector<LogicalTypeID>{LogicalTypeID::ANY};
    }

    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
    }
};

} // namespace function
} // namespace kuzu