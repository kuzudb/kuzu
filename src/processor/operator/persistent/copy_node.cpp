#include "processor/operator/persistent/copy_node.h"

#include "common/string_utils.h"
#include "storage/copier/string_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

CopyNodeSharedState::CopyNodeSharedState(uint64_t& numRows, NodeTableSchema* tableSchema,
    NodeTable* table, const CopyDescription& copyDesc, MemoryManager* memoryManager)
    : numRows{numRows}, copyDesc{copyDesc}, tableSchema{tableSchema}, table{table}, pkColumnID{0},
      hasLoggedWAL{false}, currentNodeGroupIdx{0}, isCopyTurtle{copyDesc.fileType ==
                                                                CopyDescription::FileType::TURTLE} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
}

void CopyNodeSharedState::initializePrimaryKey(const std::string& directory) {
    if (tableSchema->getPrimaryKey()->getDataType()->getLogicalTypeID() != LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(
            StorageUtils::getNodeIndexFName(directory, tableSchema->tableID, DBFileType::ORIGINAL),
            *tableSchema->getPrimaryKey()->getDataType());
        pkIndex->bulkReserve(numRows);
    }
    for (auto& property : tableSchema->properties) {
        if (property->getPropertyID() == tableSchema->getPrimaryKey()->getPropertyID()) {
            break;
        }
        pkColumnID++;
    }
}

void CopyNodeSharedState::logCopyNodeWALRecord(WAL* wal) {
    std::unique_lock xLck{mtx};
    if (!hasLoggedWAL) {
        wal->logCopyNodeRecord(table->getTableID(), table->getDataFH()->getNumPages());
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
        sharedNodeGroup->append(localNodeGroup.get(), 0 /* offsetInNodeGroup */);
    if (sharedNodeGroup->isFull()) {
        auto nodeGroupIdx = getNextNodeGroupIdxWithoutLock();
        CopyNode::writeAndResetNodeGroup(
            nodeGroupIdx, pkIndex.get(), pkColumnID, table, sharedNodeGroup.get(), isCopyTurtle);
    }
    if (numNodesAppended < localNodeGroup->getNumNodes()) {
        sharedNodeGroup->append(localNodeGroup.get(), numNodesAppended);
    }
}

void CopyNode::initGlobalStateInternal(ExecutionContext* context) {
    if (!isCopyAllowed()) {
        throw CopyException(ExceptionMessage::notAllowCopyOnNonEmptyTableException());
    }
    sharedState->initialize(copyNodeInfo.wal->getDirectory());
}

void CopyNode::executeInternal(ExecutionContext* context) {
    // CopyNode goes through UNDO log, should be logged and flushed to WAL before making changes.
    sharedState->logCopyNodeWALRecord(copyNodeInfo.wal);
    while (children[0]->getNextTuple(context)) {
        auto originalSelVector =
            resultSet->getDataChunk(copyNodeInfo.dataColumnPoses[0].dataChunkPos)->state->selVector;
        switch (sharedState->copyDesc.fileType) {
        case CopyDescription::FileType::TURTLE: {
            for (auto& dataPos : copyNodeInfo.dataColumnPoses) {
                // All tuples in the resultSet are in the same data chunk.
                auto vectorToAppend = resultSet->getValueVector(dataPos).get();
                appendUniqueValueToPKIndex(sharedState->pkIndex.get(), vectorToAppend);
                copyToNodeGroup({dataPos});
            }
        } break;
        case CopyDescription::FileType::CSV:
        case CopyDescription::FileType::PARQUET:
        case CopyDescription::FileType::NPY: {
            copyToNodeGroup(copyNodeInfo.dataColumnPoses);
        } break;
        default: {
            throw NotImplementedException{"CopyNode::executeInternal"};
        }
        }
        resultSet->getDataChunk(copyNodeInfo.dataColumnPoses[0].dataChunkPos)->state->selVector =
            std::move(originalSelVector);
    }
    if (localNodeGroup->getNumNodes() > 0) {
        sharedState->appendLocalNodeGroup(std::move(localNodeGroup));
    }
}

void CopyNode::sliceDataChunk(
    const DataChunk& dataChunk, const std::vector<DataPos>& dataColumnPoses, offset_t offset) {
    if (dataChunk.valueVectors[0]->dataType.getPhysicalType() == PhysicalTypeID::ARROW_COLUMN) {
        for (auto& dataColumnPos : dataColumnPoses) {
            ArrowColumnVector::slice(
                dataChunk.valueVectors[dataColumnPos.valueVectorPos].get(), offset);
        }
    } else {
        auto slicedSelVector = std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
        slicedSelVector->resetSelectorToValuePosBufferWithSize(
            dataChunk.state->selVector->selectedSize - offset);
        for (auto i = 0u; i < slicedSelVector->selectedSize; i++) {
            slicedSelVector->selectedPositions[i] =
                dataChunk.state->selVector->selectedPositions[i + offset];
        }
        dataChunk.state->selVector = std::move(slicedSelVector);
    }
}

void CopyNode::writeAndResetNodeGroup(node_group_idx_t nodeGroupIdx,
    PrimaryKeyIndexBuilder* pkIndex, column_id_t pkColumnID, NodeTable* table, NodeGroup* nodeGroup,
    bool isCopyTurtle) {
    nodeGroup->setNodeGroupIdx(nodeGroupIdx);
    auto startOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    if (pkIndex && !isCopyTurtle) {
        populatePKIndex(pkIndex, nodeGroup->getColumnChunk(pkColumnID), startOffset,
            nodeGroup->getNumNodes() /* startPageIdx */);
    }
    table->append(nodeGroup);
    nodeGroup->resetToEmpty();
}

void CopyNode::populatePKIndex(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, offset_t numNodes) {
    checkNonNullConstraint(chunk->getNullChunk(), numNodes);
    std::string errorPKValueStr;
    pkIndex->lock();
    try {
        switch (chunk->getDataType().getPhysicalType()) {
        case PhysicalTypeID::INT64: {
            auto numAppendedNodes = appendToPKIndex<int64_t>(pkIndex, chunk, startOffset, numNodes);
            if (numAppendedNodes < numNodes) {
                errorPKValueStr =
                    std::to_string(chunk->getValue<int64_t>(startOffset + numAppendedNodes));
            }
        } break;
        case PhysicalTypeID::STRING: {
            auto numAppendedNodes =
                appendToPKIndex<ku_string_t>(pkIndex, chunk, startOffset, numNodes);
            if (numAppendedNodes < numNodes) {
                errorPKValueStr =
                    chunk->getValue<ku_string_t>(startOffset + numAppendedNodes).getAsString();
            }
        } break;
        default: {
            throw CopyException(ExceptionMessage::invalidPKType(
                LogicalTypeUtils::dataTypeToString(chunk->getDataType())));
        }
        }
    } catch (Exception& e) {
        pkIndex->unlock();
        throw;
    }
    pkIndex->unlock();
    if (!errorPKValueStr.empty()) {
        throw CopyException(ExceptionMessage::existedPKException(errorPKValueStr));
    }
}

void CopyNode::checkNonNullConstraint(NullColumnChunk* nullChunk, offset_t numNodes) {
    for (auto posInChunk = 0u; posInChunk < numNodes; posInChunk++) {
        if (nullChunk->isNull(posInChunk)) {
            throw CopyException(ExceptionMessage::nullPKException());
        }
    }
}

void CopyNode::finalize(ExecutionContext* context) {
    if (sharedState->sharedNodeGroup) {
        auto nodeGroupIdx = sharedState->getNextNodeGroupIdx();
        writeAndResetNodeGroup(nodeGroupIdx, sharedState->pkIndex.get(), sharedState->pkColumnID,
            sharedState->table, sharedState->sharedNodeGroup.get(), sharedState->isCopyTurtle);
    }
    if (sharedState->pkIndex) {
        sharedState->pkIndex->flush();
    }
    std::unordered_set<table_id_t> connectedRelTableIDs;
    connectedRelTableIDs.insert(sharedState->tableSchema->getFwdRelTableIDSet().begin(),
        sharedState->tableSchema->getFwdRelTableIDSet().end());
    connectedRelTableIDs.insert(sharedState->tableSchema->getBwdRelTableIDSet().begin(),
        sharedState->tableSchema->getBwdRelTableIDSet().end());
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

template<>
uint64_t CopyNode::appendToPKIndex<int64_t>(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<int64_t>(i);
        if (!pkIndex->append(value, offset)) {
            return i;
        }
    }
    return numValues;
}

template<>
uint64_t CopyNode::appendToPKIndex<ku_string_t>(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, uint64_t numValues) {
    auto stringColumnChunk = (StringColumnChunk*)chunk;
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = stringColumnChunk->getValue<std::string>(i);
        if (!pkIndex->append(value.c_str(), offset)) {
            return i;
        }
    }
    return numValues;
}

void CopyNode::appendUniqueValueToPKIndex(
    storage::PrimaryKeyIndexBuilder* pkIndex, common::ValueVector* vectorToAppend) {
    auto selVector = std::make_unique<common::SelectionVector>(common::DEFAULT_VECTOR_CAPACITY);
    selVector->resetSelectorToValuePosBuffer();
    common::sel_t nextPos = 0;
    common::offset_t result;
    auto offset = StorageUtils::getStartOffsetOfNodeGroup(sharedState->getCurNodeGroupIdx()) +
                  localNodeGroup->getNumNodes();
    for (auto i = 0u; i < vectorToAppend->state->getNumSelectedValues(); i++) {
        auto uriStr = vectorToAppend->getValue<common::ku_string_t>(i).getAsString();
        if (!pkIndex->lookup((int64_t)uriStr.c_str(), result)) {
            pkIndex->append(uriStr.c_str(), offset++);
            selVector->selectedPositions[nextPos++] = i;
        }
    }
    selVector->selectedSize = nextPos;
    vectorToAppend->state->selVector = std::move(selVector);
}

void CopyNode::copyToNodeGroup(std::vector<DataPos> dataPoses) {
    auto numAppendedTuples = 0ul;
    auto numTuplesToAppend =
        resultSet->getDataChunk(dataPoses[0].dataChunkPos)->state->getNumSelectedValues();
    while (numAppendedTuples < numTuplesToAppend) {
        auto numAppendedTuplesInNodeGroup =
            localNodeGroup->append(resultSet, dataPoses, numTuplesToAppend - numAppendedTuples);
        numAppendedTuples += numAppendedTuplesInNodeGroup;
        if (localNodeGroup->isFull()) {
            node_group_idx_t nodeGroupIdx;
            nodeGroupIdx = sharedState->getNextNodeGroupIdx();
            writeAndResetNodeGroup(nodeGroupIdx, sharedState->pkIndex.get(),
                sharedState->pkColumnID, sharedState->table, localNodeGroup.get(),
                sharedState->isCopyTurtle);
        }
        if (numAppendedTuples < numTuplesToAppend) {
            sliceDataChunk(*resultSet->getDataChunk(copyNodeInfo.dataColumnPoses[0].dataChunkPos),
                copyNodeInfo.dataColumnPoses, (offset_t)numAppendedTuplesInNodeGroup);
        }
    }
}

} // namespace processor
} // namespace kuzu
