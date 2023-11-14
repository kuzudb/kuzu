#include "processor/operator/persistent/delete.h"

namespace kuzu {
namespace processor {

void DeleteNode::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& executor : executors) {
        executor->init(resultSet, context);
    }
}

bool DeleteNode::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& executor : executors) {
        executor->delete_(context);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> DeleteNode::clone() {
    std::vector<std::unique_ptr<NodeDeleteExecutor>> executorsCopy;
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return std::make_unique<DeleteNode>(
        std::move(executorsCopy), children[0]->clone(), id, paramsString);
}

void DeleteRel::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& executor : executors) {
        executor->init(resultSet, context);
    }
}

bool DeleteRel::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& executor : executors) {
        executor->delete_(context);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> DeleteRel::clone() {
    std::vector<std::unique_ptr<RelDeleteExecutor>> executorsCopy;
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return std::make_unique<DeleteRel>(
        std::move(executorsCopy), children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
