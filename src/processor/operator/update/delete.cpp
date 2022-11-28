#include "processor/operator/update/delete.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> DeleteNode::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& deleteNodeInfo : deleteNodeInfos) {
        auto nodeIDPos = deleteNodeInfo->nodeIDPos;
        auto nodeIDVector =
            resultSet->dataChunks[nodeIDPos.dataChunkPos]->valueVectors[nodeIDPos.valueVectorPos];
        nodeIDVectors.push_back(nodeIDVector.get());
        auto pkPos = deleteNodeInfo->primaryKeyPos;
        auto pkVector =
            resultSet->dataChunks[pkPos.dataChunkPos]->valueVectors[pkPos.valueVectorPos];
        primaryKeyVectors.push_back(pkVector.get());
    }
    return resultSet;
}

bool DeleteNode::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    for (auto i = 0u; i < deleteNodeInfos.size(); ++i) {
        auto nodeTable = deleteNodeInfos[i]->table;
        nodeTable->deleteNodes(nodeIDVectors[i], primaryKeyVectors[i]);
    }
    return true;
}

shared_ptr<ResultSet> DeleteRel::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& deleteRelInfo : deleteRelInfos) {
        auto srcNodePos = deleteRelInfo->srcNodePos;
        auto srcNodeIDVector =
            resultSet->dataChunks[srcNodePos.dataChunkPos]->valueVectors[srcNodePos.valueVectorPos];
        srcNodeVectors.push_back(srcNodeIDVector.get());
        auto dstNodePos = deleteRelInfo->dstNodePos;
        auto dstNodeIDVector =
            resultSet->dataChunks[dstNodePos.dataChunkPos]->valueVectors[dstNodePos.valueVectorPos];
        dstNodeVectors.push_back(dstNodeIDVector.get());
        auto relIDPos = deleteRelInfo->relIDPos;
        auto relIDVector =
            resultSet->dataChunks[relIDPos.dataChunkPos]->valueVectors[relIDPos.valueVectorPos];
        relIDVectors.push_back(relIDVector.get());
    }
    return resultSet;
}

bool DeleteRel::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    for (auto i = 0u; i < deleteRelInfos.size(); ++i) {
        auto createRelInfo = deleteRelInfos[i].get();
        auto srcNodeVector = srcNodeVectors[i];
        auto dstNodeVector = dstNodeVectors[i];
        auto relIDVector = relIDVectors[i];
        // TODO(Ziyi): you need to update rel statistics here. Though I don't think this is best
        // design, statistics update should be hidden inside deleteRel(). See how node create/delete
        // works. You can discuss this with Semih and if you decide to change, change createRel too.
        createRelInfo->table->deleteRel(srcNodeVector, dstNodeVector);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
