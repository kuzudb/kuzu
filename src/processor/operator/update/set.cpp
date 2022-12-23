#include "processor/operator/update/set.h"

namespace kuzu {
namespace processor {

void SetNodeProperty::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& info : infos) {
        auto nodeIDVector = resultSet->getValueVector(info->nodeIDPos);
        nodeIDVectors.push_back(nodeIDVector);
        info->evaluator->init(*resultSet, context->memoryManager);
    }
}

bool SetNodeProperty::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    for (auto i = 0u; i < infos.size(); ++i) {
        auto info = infos[i].get();
        info->evaluator->evaluate();
        info->column->writeValues(nodeIDVectors[i], info->evaluator->resultVector);
    }
    return true;
}

unique_ptr<PhysicalOperator> SetNodeProperty::clone() {
    vector<unique_ptr<SetNodePropertyInfo>> clonedInfos;
    for (auto& info : infos) {
        clonedInfos.push_back(info->clone());
    }
    return make_unique<SetNodeProperty>(
        std::move(clonedInfos), children[0]->clone(), id, paramsString);
}

void SetRelProperty::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& info : infos) {
        auto srcNodeIDVector = resultSet->getValueVector(info->srcNodePos);
        srcNodeVectors.push_back(srcNodeIDVector);
        auto dstNodeIDVector = resultSet->getValueVector(info->dstNodePos);
        dstNodeVectors.push_back(dstNodeIDVector);
        auto relIDVector = resultSet->getValueVector(info->relIDPos);
        relIDVectors.push_back(relIDVector);
        info->evaluator->init(*resultSet, context->memoryManager);
    }
}

bool SetRelProperty::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    throw NotImplementedException("Unimplemented SetRelProperty.");
}

unique_ptr<PhysicalOperator> SetRelProperty::clone() {
    vector<unique_ptr<SetRelPropertyInfo>> clonedInfos;
    for (auto& info : infos) {
        clonedInfos.push_back(info->clone());
    }
    return make_unique<SetRelProperty>(
        std::move(clonedInfos), children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
