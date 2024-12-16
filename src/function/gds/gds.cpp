#include "function/gds/gds.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::main;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

template<typename T>
T GDSBindInput::getLiteralVal(common::idx_t idx) const {
    auto value = params[idx]->constCast<LiteralExpression>().getValue();
    return value.getValue<T>();
}

template KUZU_API std::string GDSBindInput::getLiteralVal<std::string>(common::idx_t idx) const;
template KUZU_API uint64_t GDSBindInput::getLiteralVal<uint64_t>(common::idx_t idx) const;
template KUZU_API double GDSBindInput::getLiteralVal<double>(common::idx_t idx) const;
template KUZU_API bool GDSBindInput::getLiteralVal<bool>(common::idx_t idx) const;

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
