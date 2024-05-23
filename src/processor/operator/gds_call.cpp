#include "processor/operator/gds_call.h"

#include "binder/expression/graph_expression.h"
#include "graph/on_disk_graph.h"

using namespace kuzu::binder;
using namespace kuzu::graph;

namespace kuzu {
namespace processor {

void GDSCall::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    info.gds->init(sharedState->graph.get(), sharedState->fTable.get(), context->clientContext);
}

void GDSCall::initGlobalStateInternal(ExecutionContext* context) {
    auto graphExpr_ = info.graphExpr->constPtrCast<GraphExpression>();
    sharedState->graph = std::make_unique<OnDiskGraph>(context->clientContext,
        graphExpr_->getNodeName(), graphExpr_->getRelName());
}

void GDSCall::executeInternal(ExecutionContext*) {
    info.gds->exec();
}

} // namespace processor
} // namespace kuzu
