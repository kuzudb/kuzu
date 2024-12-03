#include "function/gds/gds.h"

#include "binder/binder.h"
#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"
#include "function/gds/gds_frontier.h"

using namespace kuzu::binder;
using namespace kuzu::main;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

std::shared_ptr<Expression> GDSAlgorithm::bindNodeOutput(Binder* binder,
    const GraphEntry& graphEntry) {
    auto node = binder->createQueryNode(NODE_COLUMN_NAME, graphEntry.nodeEntries);
    binder->addToScope(NODE_COLUMN_NAME, node);
    return node;
}

std::shared_ptr<PathLengths> GDSAlgorithm::getPathLengthsFrontier(processor::ExecutionContext* context) {
    auto tx = context->clientContext->getTx();
    auto mm = context->clientContext->getMemoryManager();
    auto graph = sharedState->graph.get();
    auto pathLengths = std::make_shared<PathLengths>(graph->getNumNodesMap(tx), mm);
    auto vc = std::make_unique<PathLengthsInitVertexCompute>(*pathLengths);
    GDSUtils::runVertexCompute(context, graph, *vc);
    return pathLengths;
}

} // namespace function
} // namespace kuzu
