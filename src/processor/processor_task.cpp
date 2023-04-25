#include "processor/processor_task.h"

#include "processor/operator/var_length_extend/recursive_join.h"
#include "processor/operator/var_length_extend/scan_bfs_level.h"
#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

// TODO(Xiyang):
static void initThreadLocalSharedStateForRecursiveJoin(PhysicalOperator& op) {
    auto threadLocalSharedState = std::make_shared<SSPThreadLocalSharedState>();
    auto& recursiveJoin = (RecursiveJoin&)op;
    recursiveJoin.setThreadLocalSharedState(threadLocalSharedState);
    auto& scanRelTable = (ScanRelTable&)*recursiveJoin.getChild(0);
    scanRelTable.setThreadLocalSharedState(*threadLocalSharedState);
    auto& scanBFSLevel = (ScanBFSLevel&)*scanRelTable.getChild(0);
    scanBFSLevel.setThreadLocalSharedState(threadLocalSharedState);
}

void ProcessorTask::run() {
    // We need the lock when cloning because multiple threads can be accessing to clone,
    // which is not thread safe
    common::lock_t lck{mtx};
    auto clonedPipelineRoot = sink->clone();
    lck.unlock();
    auto currentSink = (Sink*)clonedPipelineRoot.get();
    if (currentSink->getOperatorType() == PhysicalOperatorType::RECURSIVE_JOIN) {
        initThreadLocalSharedStateForRecursiveJoin(*currentSink);
    }
    auto resultSet = populateResultSet(currentSink, executionContext->memoryManager);
    currentSink->execute(resultSet.get(), executionContext);
}

void ProcessorTask::finalizeIfNecessary() {
    sink->finalize(executionContext);
}

static void addStructFieldsVectors(common::ValueVector* structVector, common::DataChunk* dataChunk,
    storage::MemoryManager* memoryManager) {
    auto structTypeInfo =
        reinterpret_cast<common::StructTypeInfo*>(structVector->dataType.getExtraTypeInfo());
    for (auto& childType : structTypeInfo->getChildrenTypes()) {
        auto childVector = std::make_shared<common::ValueVector>(*childType, memoryManager);
        structVector->addChildVector(childVector);
    }
}

std::unique_ptr<ResultSet> ProcessorTask::populateResultSet(
    Sink* op, storage::MemoryManager* memoryManager) {
    auto resultSetDescriptor = op->getResultSetDescriptor();
    if (resultSetDescriptor == nullptr) {
        // Some pipeline does not need a resultSet, e.g. OrderByMerge
        return nullptr;
    }
    auto numDataChunks = resultSetDescriptor->getNumDataChunks();
    auto resultSet = std::make_unique<ResultSet>(numDataChunks);
    for (auto i = 0u; i < numDataChunks; ++i) {
        auto dataChunkDescriptor = resultSetDescriptor->getDataChunkDescriptor(i);
        auto numValueVectors = dataChunkDescriptor->getNumValueVectors();
        auto dataChunk = std::make_unique<common::DataChunk>(numValueVectors);
        if (dataChunkDescriptor->isSingleState()) {
            dataChunk->state = common::DataChunkState::getSingleValueDataChunkState();
        }
        for (auto j = 0u; j < dataChunkDescriptor->getNumValueVectors(); ++j) {
            auto expression = dataChunkDescriptor->getExpression(j);
            auto vector =
                std::make_shared<common::ValueVector>(expression->dataType, memoryManager);
            if (vector->dataType.getTypeID() == common::STRUCT) {
                addStructFieldsVectors(vector.get(), dataChunk.get(), memoryManager);
            }
            dataChunk->insert(j, std::move(vector));
        }
        resultSet->insert(i, std::move(dataChunk));
    }
    return resultSet;
}

} // namespace processor
} // namespace kuzu
