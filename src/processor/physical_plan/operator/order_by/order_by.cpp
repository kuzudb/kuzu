#include "src/processor/include/physical_plan/operator/order_by/order_by.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> OrderBy::initResultSet() {
    resultSet = children[0]->initResultSet();

    // RowCollection, keyBlockEntrySizeInBytes, strKeyColInfo are constructed here because
    // they need the data type information, which is contained in the value vectors.
    RowLayout rowLayout;
    // Loop through all columns to initialize the rowCollection.
    // We need to store all columns(including keys and payload) in the rowCollection.
    for (auto i = 0u; i < orderByDataInfo.allDataPoses.size(); ++i) {
        auto dataChunkPos = orderByDataInfo.allDataPoses[i].dataChunkPos;
        auto dataChunk = this->resultSet->dataChunks[dataChunkPos];
        auto vectorPos = orderByDataInfo.allDataPoses[i].valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];

        // Check whether this col is an orderBy key column.
        bool isKeyCol = false;
        for (auto dataPos : orderByDataInfo.keyDataPoses) {
            if (orderByDataInfo.allDataPoses[i] == dataPos) {
                isKeyCol = true;
            }
        }

        // The column is an overflow column in the rowCollection
        // if it is an unflat payload column. We store a pointer to the overflow
        // valueVector in the rowCollection for overflow column.
        bool isVectorOverflow = (!isKeyCol) && (!orderByDataInfo.isVectorFlat[i]);
        rowLayout.appendField({vector->dataType,
            isVectorOverflow ? sizeof(overflow_value_t) : vector->getNumBytesPerValue(),
            isVectorOverflow});
        vectorsToAppend.push_back(vector);
    }

    // Create a rowCollection and append it to sharedState.
    rowLayout.initialize();
    localRowCollection = make_shared<RowCollection>(*context.memoryManager, rowLayout);
    rowCollectionIdx = sharedState->getNextRowCollectionIdx();
    sharedState->appendRowCollection(rowCollectionIdx, localRowCollection);

    // Loop through all key columns and calculate the offsets for string columns.
    auto encodedKeyBlockColOffset = 0ul;
    for (auto i = 0u; i < orderByDataInfo.keyDataPoses.size(); i++) {
        auto keyDataPos = orderByDataInfo.keyDataPoses[i];
        auto dataChunkPos = keyDataPos.dataChunkPos;
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        auto vectorPos = keyDataPos.valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        keyVectors.emplace_back(vector);
        if (STRING == vector->dataType) {
            // If this is a string column, we need to find the rowCollection offset for this column.
            auto rowCollectionOffset = 0ul;
            for (auto j = 0u; j < orderByDataInfo.allDataPoses.size(); j++) {
                if (orderByDataInfo.allDataPoses[j] == keyDataPos) {
                    rowCollectionOffset =
                        sharedState->rowCollections[rowCollectionIdx]->getFieldOffsetInRow(j);
                }
            }
            strKeyColInfo.emplace_back(StrKeyColInfo(
                rowCollectionOffset, encodedKeyBlockColOffset, orderByDataInfo.isAscOrder[i]));
        }
        encodedKeyBlockColOffset += OrderByKeyEncoder::getEncodingSize(vector->dataType);
    }

    // Prepare the orderByEncoder, and radix sorter
    orderByKeyEncoder = make_unique<OrderByKeyEncoder>(
        keyVectors, orderByDataInfo.isAscOrder, *context.memoryManager, rowCollectionIdx);
    radixSorter = make_unique<RadixSort>(*context.memoryManager,
        *sharedState->rowCollections[rowCollectionIdx], *orderByKeyEncoder, strKeyColInfo);

    sharedState->setStrKeyColInfo(strKeyColInfo);
    sharedState->setKeyBlockEntrySizeInBytes(orderByKeyEncoder->getKeyBlockEntrySizeInBytes());
    return resultSet;
}

void OrderBy::execute() {
    metrics->executionTime.start();
    Sink::execute();
    // Append thread-local tuples
    while (children[0]->getNextTuples()) {
        orderByKeyEncoder->encodeKeys();
        // If there is only one unflat orderBy key col, then we flatten the unflat orderBy key col
        // in rowCollection.
        // Each thread uses a unique identifier: rowCollectionID to get its private rowCollection,
        // and only appends rows to that rowCollection.
        sharedState->rowCollections[rowCollectionIdx]->append(vectorsToAppend,
            keyVectors[0]->state->isFlat() ? 1 : keyVectors[0]->state->selectedSize);
    }
    radixSorter->sortAllKeyBlocks();

    for (auto& keyBlock : orderByKeyEncoder->getKeyBlocks()) {
        if (keyBlock->numEntriesInMemBlock > 0) {
            sharedState->appendSortedKeyBlock(keyBlock);
        }
    }
    metrics->executionTime.stop();
}

void OrderBy::finalize() {}

} // namespace processor
} // namespace graphflow
