#include "processor/operator/update/delete.h"

namespace kuzu {
namespace processor {

void DeleteNode::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& deleteNodeInfo : deleteNodeInfos) {
        auto nodeIDVector = resultSet->getValueVector(deleteNodeInfo->nodeIDPos);
        nodeIDVectors.push_back(nodeIDVector.get());
        auto pkVector = resultSet->getValueVector(deleteNodeInfo->primaryKeyPos);
        primaryKeyVectors.push_back(pkVector.get());
    }
}

bool DeleteNode::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto i = 0u; i < deleteNodeInfos.size(); ++i) {
        auto nodeTable = deleteNodeInfos[i]->table;
        nodeTable->deleteNodes(nodeIDVectors[i], primaryKeyVectors[i]);
    }
    return true;
}

void DeleteRel::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& deleteRelInfo : deleteRelInfos) {
        auto srcNodeIDVector = resultSet->getValueVector(deleteRelInfo->srcNodePos);
        srcNodeVectors.push_back(srcNodeIDVector.get());
        auto dstNodeIDVector = resultSet->getValueVector(deleteRelInfo->dstNodePos);
        dstNodeVectors.push_back(dstNodeIDVector.get());
        auto relIDVector = resultSet->getValueVector(deleteRelInfo->relIDPos);
        relIDVectors.push_back(relIDVector.get());
    }
}

bool DeleteRel::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto i = 0u; i < deleteRelInfos.size(); ++i) {
        auto deleteRelInfo = deleteRelInfos[i].get();
        auto srcNodeVector = srcNodeVectors[i];
        auto dstNodeVector = dstNodeVectors[i];
        auto relIDVector = relIDVectors[i];
        deleteRelInfo->table->deleteRel(srcNodeVector, dstNodeVector, relIDVector);
        relsStatistics.updateNumRelsByValue(deleteRelInfo->table->getRelTableID(),
            -1 /* decrement numRelsPerDirectionBoundTable by 1 */);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
