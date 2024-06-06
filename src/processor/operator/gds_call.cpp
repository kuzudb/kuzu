#include "processor/operator/gds_call.h"

#include "graph/on_disk_graph.h"

using namespace kuzu::binder;
using namespace kuzu::graph;

namespace kuzu {
namespace processor {

void GDSCall::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    info.gds->init(sharedState->graph.get(), sharedState->fTable.get(), context->clientContext);
}

void GDSCall::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->graph = std::make_unique<OnDiskGraph>(context->clientContext,
        info.graphEntry.nodeTableIDs[0], info.graphEntry.relTableIDs[0]);
}

void GDSCall::executeInternal(ExecutionContext*) {
    info.gds->exec();
}

} // namespace processor
} // namespace kuzu
