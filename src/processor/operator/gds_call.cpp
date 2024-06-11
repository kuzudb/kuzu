#include "processor/operator/gds_call.h"

using namespace kuzu::binder;
using namespace kuzu::graph;

namespace kuzu {
namespace processor {

std::vector<NodeOffsetSemiMask*> GDSCall::getSemiMasks() const {
    std::vector<NodeOffsetSemiMask*> masks;
    for (auto& [_, mask] : sharedState->inputNodeOffsetMasks) {
        masks.push_back(mask.get());
    }
    return masks;
}

void GDSCall::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    info.gds->init(sharedState.get(), context->clientContext);
}

void GDSCall::executeInternal(ExecutionContext*) {
    info.gds->exec();
}

} // namespace processor
} // namespace kuzu
