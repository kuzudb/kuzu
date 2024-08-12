#include "processor/operator/gds_call.h"

using namespace kuzu::binder;
using namespace kuzu::graph;

namespace kuzu {
namespace processor {

std::string GDSCallPrintInfo::toString() const {
    return "Algorithm: " + funcName;
}

std::vector<common::NodeSemiMask*> GDSCall::getSemiMasks() const {
    std::vector<common::NodeSemiMask*> masks;
    for (auto& [_, mask] : sharedState->inputNodeOffsetMasks) {
        masks.push_back(mask.get());
    }
    return masks;
}

void GDSCall::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    info.gds->init(sharedState.get(), context->clientContext);
}

void GDSCall::executeInternal(ExecutionContext* executionContext) {
    info.gds->exec(executionContext);
}

} // namespace processor
} // namespace kuzu
