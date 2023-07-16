#include "processor/operator/copy/copy_npy_node.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void CopyNPYNode::executeInternal(ExecutionContext* context) {
    logCopyWALRecord();
    while (children[0]->getNextTuple(context)) {
        std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks;
        columnChunks.reserve(sharedState->columns.size());
        auto columnToCopy = columnIdxVector->getValue<common::vector_idx_t>(
            columnIdxVector->state->selVector->selectedPositions[0]);
        auto [startOffset, endOffset] = getStartAndEndOffset(columnToCopy);
        auto columnChunk = sharedState->columns[columnToCopy]->getInMemColumnChunk(
            startOffset, endOffset, &copyDesc);
        columnChunk->copyArrowArray(
            *ArrowColumnVector::getArrowColumn(arrowColumnVectors[columnToCopy]),
            copyStates[columnToCopy].get());
        columnChunks.push_back(std::move(columnChunk));
        flushChunksAndPopulatePKIndexSingleColumn(
            columnChunks, startOffset, endOffset, columnToCopy);
    }
}

void CopyNPYNode::flushChunksAndPopulatePKIndexSingleColumn(
    std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks, offset_t startNodeOffset,
    offset_t endNodeOffset, vector_idx_t columnToCopy) {
    sharedState->columns[columnToCopy]->flushChunk(columnChunks[0].get());
    if (sharedState->pkIndex && columnToCopy == sharedState->pkColumnID) {
        populatePKIndex(columnChunks[0].get(),
            sharedState->columns[columnToCopy]->getInMemOverflowFile(), startNodeOffset,
            (endNodeOffset - startNodeOffset + 1));
    }
}

} // namespace processor
} // namespace kuzu
