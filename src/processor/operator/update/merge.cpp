#include "processor/operator/update/merge.h"

namespace kuzu {
namespace processor {

void Merge::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    markVector = resultSet->getValueVector(markPos).get();
    for (auto& manager : nodeTableInsertManagers) {
        manager->init(resultSet, context);
    }
    for (auto& manager : relTableInsertManagers) {
        manager->init(resultSet, context);
    }
}

bool Merge::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    assert(markVector->state->isFlat());
    auto pos = markVector->state->selVector->selectedPositions[0];
    if (!markVector->isNull(pos)) {
        // TODO: handle ON MATCH
    } else {
        for (auto& manager : nodeTableInsertManagers) {
            manager->insert(transaction);
        }
        for (auto& manager : relTableInsertManagers) {
            manager->insert(transaction);
        }
        // TODO: handle ON CREATE
    }
    return true;
}

std::unique_ptr<PhysicalOperator> Merge::clone() {
    std::vector<std::unique_ptr<NodeTableInsertManager>> nodeTableInsertManagersCopy;
    for (auto& manager : nodeTableInsertManagers) {
        nodeTableInsertManagersCopy.push_back(manager->copy());
    }
    std::vector<std::unique_ptr<RelTableInsertManager>> relTableInsertManagersCopy;
    for (auto& manager : relTableInsertManagers) {
        relTableInsertManagersCopy.push_back(manager->copy());
    }
    return std::make_unique<Merge>(markPos, std::move(nodeTableInsertManagersCopy),
        std::move(relTableInsertManagersCopy), children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
