#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "processor/execution_context.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

// Edge compute function which for neighbouring nodes, determines whether a node needs to update for the next frontier
class WCCEdgeCompute : public EdgeCompute {
public:
    WCCEdgeCompute(WCCFrontierPair* frontierPair) : frontierPair{frontierPair} {}
    
    void edgeCompute(nodeID_t boundNodeID, std::span<const nodeID_t> nbrNodeIDs, std::span<const relID_t> edgeIDs, SelectionVector& mask, bool isFwd) override {
        size_t activeCount = 0;
        auto boundComponentID = frontierPair->componentIDs->getMaskValueFromCurFrontierFixedMask(boundNodeID.offset);
        // If the neighbouring node's componentID is larger, it needs to be updated.
        mask.forEach([&](auto i) {
            auto nbrComponentID = frontierPair->componentIDs->getMaskValueFromCurFrontierFixedMask(nbrNodeIDs[i].offset);
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



} // namespace function
} // namespace kuzu