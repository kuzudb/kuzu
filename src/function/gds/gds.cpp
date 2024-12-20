#include "function/gds/gds.h"

#include "binder/binder.h"
#include "common/exception/binder.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::main;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

graph::GraphEntry GDSAlgorithm::bindGraphEntry(main::ClientContext& context,
    const std::string& name) {
    if (!context.getGraphEntrySetUnsafe().hasGraph(name)) {
        throw BinderException(stringFormat("Cannot find graph {}.", name));
    }
    return context.getGraphEntrySetUnsafe().getEntry(name);
}

std::shared_ptr<Expression> GDSAlgorithm::bindNodeOutput(Binder* binder,
    const std::vector<catalog::TableCatalogEntry*>& nodeEntries) {
    auto node = binder->createQueryNode(NODE_COLUMN_NAME, nodeEntries);
    binder->addToScope(NODE_COLUMN_NAME, node);
    return node;
}

std::shared_ptr<PathLengths> GDSAlgorithm::getPathLengthsFrontier(
    processor::ExecutionContext* context, uint16_t val) {
    auto tx = context->clientContext->getTx();
    auto mm = context->clientContext->getMemoryManager();
    auto graph = sharedState->graph.get();
    auto pathLengths = std::make_shared<PathLengths>(graph->getNumNodesMap(tx), mm);
    auto vc = std::make_unique<PathLengthsInitVertexCompute>(*pathLengths, val);
    GDSUtils::runVertexCompute(context, graph, *vc);
    return pathLengths;
}

} // namespace function
} // namespace kuzu
