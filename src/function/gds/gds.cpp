#include "function/gds/gds.h"

#include "binder/binder.h"

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

} // namespace function
} // namespace kuzu
