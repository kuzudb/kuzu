#include "processor/operator/update/create.h"

#include "storage/copier/node_group.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void CreateNode::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& manager : managers) {
        manager->init(resultSet, context);
    }
}

bool CreateNode::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& manager : managers) {
        manager->insert(transaction);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> CreateNode::clone() {
    std::vector<std::unique_ptr<NodeTableInsertManager>> managersCopy;
    for (auto& manager : managers) {
        managersCopy.push_back(manager->copy());
    }
    return std::make_unique<CreateNode>(
        std::move(managersCopy), children[0]->clone(), id, paramsString);
}

void CreateRel::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& manager : managers) {
        manager->init(resultSet, context);
    }
}

bool CreateRel::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& manager : managers) {
        manager->insert(transaction);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> CreateRel::clone() {
    std::vector<std::unique_ptr<RelTableInsertManager>> managersCopy;
    for (auto& manager : managers) {
        managersCopy.push_back(manager->copy());
    }
    return std::make_unique<CreateRel>(
        std::move(managersCopy), children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
