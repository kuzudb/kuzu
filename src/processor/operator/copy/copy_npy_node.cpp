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
        auto [startOffset, endOffset] = getStartAndEndRowIdx(columnToCopy);
        auto [filePath, startRowIdxInFile] = getFilePathAndRowIdxInFile();
        auto columnChunk = sharedState->columns[columnToCopy]->createInMemColumnChunk(
            startOffset, endOffset, &copyDesc);
        columnChunk->copyArrowArray(
            *ArrowColumnVector::getArrowColumn(arrowColumnVectors[columnToCopy]),
            copyStates[columnToCopy].get());
        columnChunks.push_back(std::move(columnChunk));
        flushChunksAndPopulatePKIndexSingleColumn(
            columnChunks, startOffset, endOffset, columnToCopy, filePath, startRowIdxInFile);
    }
}

void CopyNPYNode::flushChunksAndPopulatePKIndexSingleColumn(
    std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks, offset_t startNodeOffset,
    offset_t endNodeOffset, vector_idx_t columnToCopy, const std::string& filePath,
    row_idx_t startRowIdxInFile) {
    sharedState->columns[columnToCopy]->flushChunk(columnChunks[0].get());
    if (sharedState->pkIndex && columnToCopy == sharedState->pkColumnID) {
        populatePKIndex(columnChunks[0].get(),
            sharedState->columns[columnToCopy]->getInMemOverflowFile(), startNodeOffset,
            (endNodeOffset - startNodeOffset + 1), filePath, startRowIdxInFile);
    }
}

} // namespace processor
} // namespace kuzu
