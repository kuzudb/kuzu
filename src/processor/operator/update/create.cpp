#include "processor/operator/update/create.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void CreateNode::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& executor : executors) {
        executor->init(resultSet, context);
    }
}

bool CreateNode::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& executor : executors) {
        executor->insert(transaction);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> CreateNode::clone() {
    std::vector<std::unique_ptr<NodeInsertExecutor>> executorsCopy;
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return std::make_unique<CreateNode>(
        std::move(executorsCopy), children[0]->clone(), id, paramsString);
}

void CreateRel::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& executor : executors) {
        executor->init(resultSet, context);
    }
}

bool CreateRel::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& executor : executors) {
        executor->insert(transaction);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> CreateRel::clone() {
    std::vector<std::unique_ptr<RelInsertExecutor>> executorsCopy;
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return std::make_unique<CreateRel>(
        std::move(executorsCopy), children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
