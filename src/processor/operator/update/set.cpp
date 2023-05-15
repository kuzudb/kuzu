#include "processor/operator/update/set.h"

namespace kuzu {
namespace processor {

void SetNodeProperty::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& info : infos) {
        auto nodeIDVector = resultSet->getValueVector(info->nodeIDPos);
        nodeIDVectors.push_back(nodeIDVector.get());
        info->evaluator->init(*resultSet, context->memoryManager);
    }
}

bool SetNodeProperty::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto i = 0u; i < infos.size(); ++i) {
        auto info = infos[i].get();
        info->evaluator->evaluate();
        info->column->write(nodeIDVectors[i], info->evaluator->resultVector.get());
    }
    return true;
}

std::unique_ptr<PhysicalOperator> SetNodeProperty::clone() {
    std::vector<std::unique_ptr<SetNodePropertyInfo>> clonedInfos;
    for (auto& info : infos) {
        clonedInfos.push_back(info->clone());
    }
    return make_unique<SetNodeProperty>(
        std::move(clonedInfos), children[0]->clone(), id, paramsString);
}

void SetRelProperty::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& info : infos) {
        auto srcNodeIDVector = resultSet->getValueVector(info->srcNodePos);
        srcNodeVectors.push_back(srcNodeIDVector.get());
        auto dstNodeIDVector = resultSet->getValueVector(info->dstNodePos);
        dstNodeVectors.push_back(dstNodeIDVector.get());
        auto relIDVector = resultSet->getValueVector(info->relIDPos);
        relIDVectors.push_back(relIDVector.get());
        info->evaluator->init(*resultSet, context->memoryManager);
    }
}

bool SetRelProperty::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto i = 0u; i < infos.size(); ++i) {
        auto info = infos[i].get();
        info->evaluator->evaluate();
        info->table->updateRel(srcNodeVectors[i], dstNodeVectors[i], relIDVectors[i],
            info->evaluator->resultVector.get(), info->propertyId);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> SetRelProperty::clone() {
    std::vector<std::unique_ptr<SetRelPropertyInfo>> clonedInfos;
    for (auto& info : infos) {
        clonedInfos.push_back(info->clone());
    }
    return make_unique<SetRelProperty>(
        std::move(clonedInfos), children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
