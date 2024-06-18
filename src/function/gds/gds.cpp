#include "function/gds/gds.h"

#include "binder/binder.h"
#include "function/gds/parallel_utils.h"
#include "processor/operator/gds_call.h"

using namespace kuzu::binder;
using namespace kuzu::main;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

void GDSAlgorithm::init(GDSCallSharedState* sharedState_, ClientContext* context) {
    sharedState = sharedState_;
    initLocalState(context);
}

std::shared_ptr<Expression> GDSAlgorithm::bindNodeOutput(Binder* binder, GraphEntry& graphEntry) {
    auto node = binder->createQueryNode(NODE_COLUMN_NAME, graphEntry.nodeTableIDs);
    binder->addToScope(NODE_COLUMN_NAME, node);
    return node;
}

void GDSAlgorithm::setTaskSchedulerAndOperatorIDForParallelization(common::TaskScheduler* taskScheduler,
    uint32_t operatorID) {
    parallelUtils = std::make_shared<ParallelUtils>(taskScheduler, operatorID);
}

} // namespace function
} // namespace kuzu
