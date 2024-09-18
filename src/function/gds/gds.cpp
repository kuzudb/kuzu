#include "function/gds/gds.h"

#include "binder/binder.h"
#include "processor/operator/gds_call.h"

using namespace kuzu::binder;
using namespace kuzu::main;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

GDSAlgorithm::GDSAlgorithm(const GDSAlgorithm& other) {
    if (other.bindData != nullptr) {
        bindData = other.bindData->copy();
    }
    sharedState = other.sharedState;
}

std::shared_ptr<Expression> GDSAlgorithm::bindNodeOutput(Binder* binder, const GraphEntry& graphEntry) {
    auto node = binder->createQueryNode(NODE_COLUMN_NAME, graphEntry.nodeTableIDs);
    binder->addToScope(NODE_COLUMN_NAME, node);
    return node;
}

} // namespace function
} // namespace kuzu
