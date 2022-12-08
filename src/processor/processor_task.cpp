#include "processor/processor_task.h"

namespace kuzu {
namespace processor {

void ProcessorTask::run() {
    // We need the lock when cloning because multiple threads can be accessing to clone,
    // which is not thread safe
    lock_t lck{mtx};
    auto clonedPipelineRoot = sink->clone();
    lck.unlock();
    auto currentSink = (Sink*)clonedPipelineRoot.get();
    auto resultSet = populateResultSet(currentSink, executionContext->memoryManager);
    currentSink->execute(resultSet.get(), executionContext);
}

void ProcessorTask::finalizeIfNecessary() {
    sink->finalize(executionContext);
}

unique_ptr<ResultSet> ProcessorTask::populateResultSet(Sink* op, MemoryManager* memoryManager) {
    auto resultSetDescriptor = op->getResultSetDescriptor();
    if (resultSetDescriptor == nullptr) {
        // Some pipeline does not need a resultSet, e.g. OrderByMerge
        return nullptr;
    }
    auto numDataChunks = resultSetDescriptor->getNumDataChunks();
    auto resultSet = make_unique<ResultSet>(numDataChunks);
    for (auto i = 0u; i < numDataChunks; ++i) {
        auto dataChunkDescriptor = resultSetDescriptor->getDataChunkDescriptor(i);
        auto numValueVectors = dataChunkDescriptor->getNumValueVectors();
        auto dataChunk = make_unique<DataChunk>(numValueVectors);
        if (dataChunkDescriptor->isSingleState()) {
            dataChunk->state = DataChunkState::getSingleValueDataChunkState();
        }
        for (auto j = 0u; j < dataChunkDescriptor->getNumValueVectors(); ++j) {
            auto expression = dataChunkDescriptor->getExpression(j);
            auto vector = make_shared<ValueVector>(expression->dataType, memoryManager);
            dataChunk->insert(j, std::move(vector));
        }
        resultSet->insert(i, std::move(dataChunk));
    }
    return resultSet;
}

} // namespace processor
} // namespace kuzu
