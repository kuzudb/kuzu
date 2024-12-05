#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace function {
struct KCoreInitEdgeCompute : public EdgeCompute {
    KCoreFrontierPair* frontierPair;

    explicit KCoreInitEdgeCompute(KCoreFrontierPair* frontierPair) : frontierPair{frontierPair} {}

    std::vector<common::nodeID_t> edgeCompute(common::nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk, bool) override {
        std::vector<common::nodeID_t> result;
        uint64_t nbrAmount = 0;
        chunk.forEach([&](auto nbrNodeID, auto) {nbrAmount++;
        result.push_back(nbrNodeID);});
        frontierPair->updateCurSmallestDegree(frontierPair->addToVertexDegree(boundNodeID, nbrAmount) + nbrAmount);
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<KCoreInitEdgeCompute>(frontierPair);
    }
};

struct KCoreEdgeCompute : public EdgeCompute {
    KCoreFrontierPair* frontierPair;

    explicit KCoreEdgeCompute(KCoreFrontierPair* frontierPair) : frontierPair{frontierPair} {}

    std::vector<common::nodeID_t> edgeCompute(common::nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk, bool) override {
        std::vector<common::nodeID_t> result;
        if (frontierPair->getVertexDegree(boundNodeID) == frontierPair->getSmallestDegree()) {
            frontierPair->setKValue(boundNodeID, frontierPair->getSmallestDegree());
            chunk.forEach([&](auto nbrNodeID, auto) {

                result.push_back(nbrNodeID);
                frontierPair->updateNextSmallestDegree(frontierPair->removeFromVertex(boundNodeID) - 1);
            });
        return result;
    }
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<KCoreEdgeCompute>(frontierPair);
    }
};

class KCoreVertexCompute : public VertexCompute {
public:
    KCoreVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        const RJCompState& compState)
        : mm{mm}, sharedState{sharedState}, compState{compState}, outputWriter{mm} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~KCoreVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(common::table_id_t) override { return true; }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override {
        // const auto& frontierPair = compState.frontierPair->ptrCast<KCoreFrontierPair>();
        // auto id = frontierPair->getComponentID(curNodeID);
        // outputWriter.materialize(curNodeID, id, *localFT);

        for (auto nodeID : chunk.getNodeIDs()) {
            if (sharedState->exceedLimit()) {
                return;
            }
            if (writer->skip(nodeID)) {
                continue;
            }
            auto kValue = compState.frontierPair->ptrCast<KCoreFrontierPair>
            writer->write(*localFT, )
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WCCVertexCompute>(mm, sharedState, compState);
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    const RJCompState& compState;
    KCoreOutputWriter outputWriter;
    processor::FactorizedTable* localFT;
};
}
}