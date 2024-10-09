#include "processor/operator/property_collector.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string PropertyCollectorPrintInfo::toString() const {
    return "Operators: " + operatorName;
}

void PropertyCollector::initGlobalStateInternal(ExecutionContext* /*context*/) {}

void PropertyCollector::initLocalStateInternal(ResultSet* resultSet, ExecutionContext*) {
    localState.nodeIDVector = resultSet->getValueVector(info.nodeIDPos).get();
    localState.propertyVector = resultSet->getValueVector(info.propPos).get();
}

bool PropertyCollector::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    std::lock_guard<std::mutex> guard{sharedState->mtx};
    auto state = localState.nodeIDVector->state;
    for (auto i = 0u; i < state->getSelSize(); i++) {
        auto pos = state->getSelVector()[i];
        auto nodeID = localState.nodeIDVector->getValue<internalID_t>(pos);
        auto property = localState.propertyVector->getValue<uint64_t>(pos);
        KU_ASSERT(!sharedState->nodeProperty.contains(nodeID));
        sharedState->nodeProperty.emplace(nodeID, property);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
