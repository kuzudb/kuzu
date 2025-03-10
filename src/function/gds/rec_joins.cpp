#include "function/gds/rec_joins.h"

#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

RJBindData::RJBindData(const RJBindData& other) {
    graphEntry = other.graphEntry.copy();
    nodeInput = other.nodeInput;
    nodeOutput = other.nodeOutput;
    lowerBound = other.lowerBound;
    upperBound = other.upperBound;
    semantic = other.semantic;
    extendDirection = other.extendDirection;
    flipPath = other.flipPath;
    writePath = other.writePath;
    directionExpr = other.directionExpr;
    lengthExpr = other.lengthExpr;
    pathNodeIDsExpr = other.pathNodeIDsExpr;
    pathEdgeIDsExpr = other.pathEdgeIDsExpr;
    weightPropertyExpr = other.weightPropertyExpr;
    weightOutputExpr = other.weightOutputExpr;
}

PathsOutputWriterInfo RJBindData::getPathWriterInfo() const {
    auto info = PathsOutputWriterInfo();
    info.semantic = semantic;
    info.lowerBound = lowerBound;
    info.flipPath = flipPath;
    info.writeEdgeDirection = writePath && extendDirection == ExtendDirection::BOTH;
    info.writePath = writePath;
    return info;
}

std::unique_ptr<BFSGraph> RJAlgorithm::getBFSGraph(processor::ExecutionContext* context,
    graph::Graph* graph) {
    auto tx = context->clientContext->getTransaction();
    auto mm = context->clientContext->getMemoryManager();
    auto bfsGraph = std::make_unique<BFSGraph>(graph->getMaxOffsetMap(tx), mm);
    auto vc = std::make_unique<BFSGraphInitVertexCompute>(*bfsGraph);
    GDSUtils::runVertexCompute(context, graph, *vc);
    return bfsGraph;
}

} // namespace function
} // namespace kuzu
