#include "processor/operator/var_length_extend/scan_bfs_level.h"

namespace kuzu {
namespace processor {

void ScanBFSLevel::initGlobalStateInternal(ExecutionContext* context) {
    globalSharedState->initTable(context->memoryManager, populateTableSchema());
}

void ScanBFSLevel::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    maxNodeOffset = nodeTable->getMaxNodeOffset(transaction);
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDVectorPos);
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDVectorPos);
    distanceVector = resultSet->getValueVector(distanceVectorPos);
    for (auto& payload : payloadInfos) {
        vectorsToCollect.push_back(resultSet->getValueVector(payload.dataPos).get());
    }
    threadLocalSharedState->sspMorsel = std::make_unique<SSPMorsel>(maxNodeOffset);
    threadLocalSharedState->fTable =
        std::make_unique<FactorizedTable>(context->memoryManager, populateTableSchema());
}

bool ScanBFSLevel::getNextTuplesInternal(ExecutionContext* context) {
    auto sspMorsel = threadLocalSharedState->sspMorsel.get();
    while (true) {
        if (sspMorsel->isComplete(upperBound, maxNodeOffset)) {
            writeDstNodeIDsAndDstToFTable();
            if (!children[0]->getNextTuple(context)) {
                return false;
            }
            sspMorsel->resetState(maxNodeOffset);
            assert(srcNodeIDVector->state->isFlat());
            auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
                srcNodeIDVector->state->selVector->selectedPositions[0]);
            sspMorsel->markSrc(nodeID.offset);
        }
        auto nodeOffset = sspMorsel->getNextNodeOffset();
        if (nodeOffset != common::INVALID_NODE_OFFSET) {
            // Found a starting node from current BFS level.
            threadLocalSharedState->tmpSrcNodeIDVector->setValue(
                0, common::nodeID_t{nodeOffset, nodeTable->getTableID()});
            break;
        } else {
            // Otherwise move to the next BFS level.
            sspMorsel->moveNextLevelAsCurrentLevel(upperBound);
        }
    }
    return true;
}

void ScanBFSLevel::writeDstNodeIDsAndDstToFTable() {
    auto sspMorsel = threadLocalSharedState->sspMorsel.get();
    if (sspMorsel->numVisitedNodes == 0) {
        return;
    }
    // Skip
    sspMorsel->visitedNodes[sspMorsel->startOffset] = NOT_VISITED;
    sspMorsel->numVisitedNodes--;
    auto numDstWritten = 0u;
    common::offset_t currentOffset = 0u;
    while (numDstWritten != sspMorsel->numVisitedNodes) {
        auto sizeToWrite = std::min<uint64_t>(
            common::DEFAULT_VECTOR_CAPACITY, sspMorsel->numVisitedNodes - numDstWritten);
        writeDstNodeIDsAndDstToVector(currentOffset, sizeToWrite);
        numDstWritten += sizeToWrite;
        threadLocalSharedState->fTable->append(vectorsToCollect);
    }
}

void ScanBFSLevel::writeDstNodeIDsAndDstToVector(common::offset_t& currentOffset, size_t size) {
    auto sspMorsel = threadLocalSharedState->sspMorsel.get();
    auto sizeWritten = 0u;
    while (sizeWritten != size) {
        if (sspMorsel->visitedNodes[currentOffset] == NOT_VISITED) {
            currentOffset++;
            continue;
        }
        dstNodeIDVector->setValue<common::nodeID_t>(
            sizeWritten, common::nodeID_t{currentOffset, nodeTable->getTableID()});
        distanceVector->setValue<int64_t>(sizeWritten, sspMorsel->distance[currentOffset]);
        sizeWritten++;
        currentOffset++;
    }
    dstNodeIDVector->state->initOriginalAndSelectedSize(sizeWritten);
}

std::unique_ptr<FactorizedTableSchema> ScanBFSLevel::populateTableSchema() {
    auto tableSchema = std::make_unique<FactorizedTableSchema>();
    for (auto& payloadInfo : payloadInfos) {
        auto columnSchema = std::make_unique<ColumnSchema>(!payloadInfo.isFlat,
            payloadInfo.dataPos.dataChunkPos, common::Types::getDataTypeSize(payloadInfo.dataType));
        tableSchema->appendColumn(std::move(columnSchema));
    }
    return tableSchema;
}

} // namespace processor
} // namespace kuzu
