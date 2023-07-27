#include "processor/operator/copy/copy_node.h"

#include "common/string_utils.h"
#include "storage/store/var_sized_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

CopyNodeSharedState::CopyNodeSharedState(uint64_t& numRows, NodeTableSchema* tableSchema,
    NodeTable* table, const common::CopyDescription& copyDesc, MemoryManager* memoryManager)
    : numRows{numRows}, copyDesc{copyDesc}, tableSchema{tableSchema}, table{table}, pkColumnID{0},
      hasLoggedWAL{false}, currentNodeGroupIdx{0} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
}

void CopyNodeSharedState::initializePrimaryKey(const std::string& directory) {
    if (tableSchema->getPrimaryKey().dataType.getLogicalTypeID() != LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(
            StorageUtils::getNodeIndexFName(directory, tableSchema->tableID, DBFileType::ORIGINAL),
            tableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(numRows);
    }
    for (auto& property : tableSchema->properties) {
        if (property.propertyID == tableSchema->getPrimaryKey().propertyID) {
            break;
        }
        pkColumnID++;
    }
}

void CopyNodeSharedState::logCopyNodeWALRecord(WAL* wal) {
    std::unique_lock xLck{mtx};
    if (!hasLoggedWAL) {
        wal->logCopyNodeRecord(table->getTableID(), table->getNodeGroupsDataFH()->getNumPages());
        wal->flushAllPages();
        hasLoggedWAL = true;
    }
}

CopyNode::CopyNode(std::shared_ptr<CopyNodeSharedState> sharedState, CopyNodeInfo copyNodeInfo,
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
    std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
    : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_NODE, std::move(child), id,
          paramsString},
      sharedState{std::move(sharedState)}, copyNodeInfo{std::move(copyNodeInfo)} {}

void CopyNodeSharedState::appendLocalNodeGroup(std::unique_ptr<NodeGroup> localNodeGroup) {
    std::unique_lock xLck{mtx};
    if (!sharedNodeGroup) {
        sharedNodeGroup = std::move(localNodeGroup);
        return;
    }
    auto numNodesAppended =
        sharedNodeGroup->appendNodeGroup(localNodeGroup.get(), 0 /* offsetInNodeGroup */);
    if (sharedNodeGroup->getNumNodes() == StorageConstants::NODE_GROUP_SIZE) {
        auto nodeGroupIdx = getNextNodeGroupIdxWithoutLock();
        sharedNodeGroup->setNodeGroupIdx(nodeGroupIdx);
        CopyNode::appendNodeGroupToTableAndPopulateIndex(
            table, sharedNodeGroup.get(), pkIndex.get(), pkColumnID);
    }
    if (numNodesAppended < localNodeGroup->getNumNodes()) {
        sharedNodeGroup->appendNodeGroup(localNodeGroup.get(), numNodesAppended);
    }
}

void CopyNode::initGlobalStateInternal(ExecutionContext* context) {
    if (!isCopyAllowed()) {
        throw CopyException("COPY commands can only be executed once on a table.");
    }
    sharedState->initialize(copyNodeInfo.wal->getDirectory());
}

void CopyNode::executeInternal(ExecutionContext* context) {
    // CopyNode goes through UNDO log, should be logged and flushed to WAL before making changes.
    sharedState->logCopyNodeWALRecord(copyNodeInfo.wal);
    while (children[0]->getNextTuple(context)) {
        auto dataChunkToCopy = resultSet->getDataChunk(0);
        // All tuples in the resultSet are in the same data chunk.
        auto numTuplesToAppend = ArrowColumnVector::getArrowColumn(
        auto nodeGroupOffset =
            nodeGroupOffsetVector->getValue<int64_t>(nodeGroupOffsetVector->state->selVector->selectedPositions[0]) -
            1;
            resultSet->getValueVector(copyNodeInfo.dataColumnPoses[0]).get())
                                     ->length();
        uint64_t numAppendedTuples = 0;
        while (numAppendedTuples < numTuplesToAppend) {
            numAppendedTuples += localNodeGroup->append(
                resultSet, copyNodeInfo.dataColumnPoses, numTuplesToAppend - numAppendedTuples);
            if (localNodeGroup->getNumNodes() == StorageConstants::NODE_GROUP_SIZE) {
                // Current node group is full, flush it and reset it to empty.
                auto nodeGroupIdx = nodeGroupOffset / StorageConstants::NODE_GROUP_SIZE - 1;
                localNodeGroup->setNodeGroupIdx(nodeGroupIdx);
                appendNodeGroupToTableAndPopulateIndex(sharedState->table, localNodeGroup.get(),
                    sharedState->pkIndex.get(), sharedState->pkColumnID);
            }
            if (numAppendedTuples < numTuplesToAppend) {
                auto slicedChunk = sliceDataVectorsInDataChunk(*dataChunkToCopy,
                    copyNodeInfo.dataColumnPoses, (int64_t)numAppendedTuples,
                    (int64_t)(numTuplesToAppend - numAppendedTuples));
                resultSet->dataChunks[0] = slicedChunk;
            }
        }
    }
    // Append left data in the local node group to the shared one.
    if (localNodeGroup->getNumNodes() > 0) {
        sharedState->appendLocalNodeGroup(std::move(localNodeGroup));
    }
}

std::shared_ptr<DataChunk> CopyNode::sliceDataVectorsInDataChunk(const DataChunk& dataChunkToSlice,
    const std::vector<DataPos>& dataColumnPoses, int64_t offset, int64_t length) {
    auto slicedChunk =
        std::make_shared<DataChunk>(dataChunkToSlice.getNumValueVectors(), dataChunkToSlice.state);
    for (auto& dataPos : dataColumnPoses) {
        slicedChunk->valueVectors[dataPos.valueVectorPos] =
            std::make_shared<ValueVector>(LogicalTypeID::ARROW_COLUMN);
    }
    for (auto& dataColumnPose : dataColumnPoses) {
        assert(dataColumnPose.dataChunkPos == 0);
        auto vectorPos = dataColumnPose.valueVectorPos;
        ArrowColumnVector::slice(dataChunkToSlice.valueVectors[vectorPos].get(),
            slicedChunk->valueVectors[vectorPos].get(), offset, length);
    }
    return slicedChunk;
}

void CopyNode::appendNodeGroupToTableAndPopulateIndex(NodeTable* table, NodeGroup* nodeGroup,
    PrimaryKeyIndexBuilder* pkIndex, column_id_t pkColumnID) {
    auto numNodes = nodeGroup->getNumNodes();
    auto startOffset = nodeGroup->getNodeGroupIdx() << StorageConstants::NODE_GROUP_SIZE_LOG2;
    if (pkIndex) {
        populatePKIndex(pkIndex, nodeGroup->getColumnChunk(pkColumnID), startOffset, numNodes);
    }
    table->appendNodeGroup(nodeGroup);
    nodeGroup->resetToEmpty();
}

void CopyNode::populatePKIndex(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, offset_t numNodes) {
    // First, check if there is any nulls.
    auto nullChunk = chunk->getNullChunk();
    for (auto posInChunk = 0u; posInChunk < numNodes; posInChunk++) {
        if (nullChunk->isNull(posInChunk)) {
            throw CopyException("Primary key cannot be null.");
        }
    }
    // No nulls, so we can populate the index with actual values.
    pkIndex->lock();
    try {
        appendToPKIndex(pkIndex, chunk, startOffset, numNodes);
    } catch (Exception& e) {
        pkIndex->unlock();
        throw;
    }
    pkIndex->unlock();
}

void CopyNode::finalize(ExecutionContext* context) {
    if (sharedState->sharedNodeGroup) {
        auto nodeGroupIdx = sharedState->getNextNodeGroupIdx();
        sharedState->sharedNodeGroup->setNodeGroupIdx(nodeGroupIdx);
        appendNodeGroupToTableAndPopulateIndex(sharedState->table,
            sharedState->sharedNodeGroup.get(), sharedState->pkIndex.get(),
            sharedState->pkColumnID);
    }
    if (sharedState->pkIndex) {
        sharedState->pkIndex->flush();
    }
    std::unordered_set<table_id_t> connectedRelTableIDs;
    connectedRelTableIDs.insert(sharedState->tableSchema->fwdRelTableIDSet.begin(),
        sharedState->tableSchema->fwdRelTableIDSet.end());
    connectedRelTableIDs.insert(sharedState->tableSchema->bwdRelTableIDSet.begin(),
        sharedState->tableSchema->bwdRelTableIDSet.end());
    for (auto relTableID : connectedRelTableIDs) {
        copyNodeInfo.relsStore->getRelTable(relTableID)
            ->batchInitEmptyRelsForNewNodes(relTableID, sharedState->numRows);
    }
    sharedState->table->getNodeStatisticsAndDeletedIDs()->setNumTuplesForTable(
        sharedState->table->getTableID(), sharedState->numRows);
    auto outputMsg = StringUtils::string_format("{} number of tuples has been copied to table: {}.",
        sharedState->numRows, sharedState->tableSchema->tableName.c_str());
    FactorizedTableUtils::appendStringToTable(
        sharedState->fTable.get(), outputMsg, context->memoryManager);
}

void CopyNode::appendToPKIndex(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, uint64_t numValues) {
    switch (chunk->getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        for (auto i = 0u; i < numValues; i++) {
            auto offset = i + startOffset;
            auto value = chunk->getValue<int64_t>(i);
            pkIndex->append(value, offset);
        }
    } break;
    case LogicalTypeID::STRING: {
        auto varSizedChunk = (VarSizedColumnChunk*)chunk;
        for (auto i = 0u; i < numValues; i++) {
            auto offset = i + startOffset;
            auto value = varSizedChunk->getValue<std::string>(i);
            pkIndex->append(value.c_str(), offset);
        }
    } break;
    default: {
        throw NotImplementedException("CopyNode::appendToPKIndex");
    }
    }
}

} // namespace processor
} // namespace kuzu
