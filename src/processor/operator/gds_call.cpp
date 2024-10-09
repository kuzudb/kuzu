#include "processor/operator/gds_call.h"

using namespace kuzu::binder;
using namespace kuzu::graph;

namespace kuzu {
namespace processor {

std::string GDSCallPrintInfo::toString() const {
    return "Algorithm: " + funcName;
}
mask_vector GDSCall::getSemiMasks() const {
    std::vector<std::shared_ptr<common::RoaringBitMapSemiMask>> masks;
    for (auto& [_, mask] : sharedState->inputNodeOffsetMasks) {
        masks.push_back(mask);
    }
    return masks;
}

void GDSCall::executeInternal(ExecutionContext* executionContext) {
    gds->exec(executionContext);
}

} // namespace processor
} // namespace kuzu
