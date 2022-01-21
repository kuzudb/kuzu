#include "src/processor/include/physical_plan/operator/order_by/order_by.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> OrderBy::initResultSet() {
    resultSet = children[0]->initResultSet();

    // FactorizedTable, keyBlockEntrySizeInBytes, stringAndUnstructuredKeyColInfo are constructed
    // here because they need the data type information, which is contained in the value vectors.
    TupleSchema tupleSchema;
    // Loop through all columns to initialize the factorizedTable.
    // We need to store all columns(including keys and payload) in the factorizedTable.
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

        // The column is an unflat column in the factorizedTable
        // if it is an unflat payload column. We store a pointer to the overflow
        // valueVector in the factorizedTable for unflat columns.
        bool isUnflat = (!isKeyCol) && (!orderByDataInfo.isVectorFlat[i]);
        tupleSchema.appendField({vector->dataType, isUnflat, dataChunkPos});
        vectorsToAppend.push_back(vector);
    }

    // Create a factorizedTable and append it to sharedState.
    tupleSchema.initialize();
    localFactorizedTable = make_shared<FactorizedTable>(*context.memoryManager, tupleSchema);
    factorizedTableIdx = sharedState->getNextFactorizedTableIdx();
    sharedState->appendFactorizedTable(factorizedTableIdx, localFactorizedTable);

    // Loop through all key columns and calculate the offsets for string and unstructured columns.
    auto encodedKeyBlockColOffset = 0ul;
    for (auto i = 0u; i < orderByDataInfo.keyDataPoses.size(); i++) {
        auto keyDataPos = orderByDataInfo.keyDataPoses[i];
        auto dataChunkPos = keyDataPos.dataChunkPos;
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        auto vectorPos = keyDataPos.valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        keyVectors.emplace_back(vector);
        if (STRING == vector->dataType || UNSTRUCTURED == vector->dataType) {
            // If this is a string or unstructured column, we need to find the
            // factorizedTable offset for this column.
            auto factorizedTableColIdx = 0ul;
            for (auto j = 0u; j < orderByDataInfo.allDataPoses.size(); j++) {
                if (orderByDataInfo.allDataPoses[j] == keyDataPos) {
                    factorizedTableColIdx = j;
                }
            }
            stringAndUnstructuredKeyColInfo.emplace_back(
                StringAndUnstructuredKeyColInfo(factorizedTableColIdx, encodedKeyBlockColOffset,
                    orderByDataInfo.isAscOrder[i], STRING == vector->dataType));
        }
        encodedKeyBlockColOffset += OrderByKeyEncoder::getEncodingSize(vector->dataType);
    }

    // Prepare the orderByEncoder, and radix sorter
    orderByKeyEncoder = make_unique<OrderByKeyEncoder>(
        keyVectors, orderByDataInfo.isAscOrder, *context.memoryManager, factorizedTableIdx);
    radixSorter = make_unique<RadixSort>(*context.memoryManager, *localFactorizedTable,
        *orderByKeyEncoder, stringAndUnstructuredKeyColInfo);

    sharedState->setStringAndUnstructuredKeyColInfo(stringAndUnstructuredKeyColInfo);
    sharedState->setKeyBlockEntrySizeInBytes(orderByKeyEncoder->getKeyBlockEntrySizeInBytes());
    return resultSet;
}

void OrderBy::execute() {
    metrics->executionTime.start();
    Sink::execute();
    // Append thread-local tuples
    while (children[0]->getNextTuples()) {
        for (auto i = 0u; i < resultSet->multiplicity; i++) {
            orderByKeyEncoder->encodeKeys();
            // If there is only one unflat orderBy key col, then we flatten the unflat orderBy key
            // col in factorizedTable. Each thread uses a unique identifier: factorizedTableIdx to
            // get its private factorizedTable, and only appends tuples to that factorizedTable.
            localFactorizedTable->append(vectorsToAppend,
                keyVectors[0]->state->isFlat() ? 1 : keyVectors[0]->state->selectedSize);
        }
    }

    for (auto& keyBlock : orderByKeyEncoder->getKeyBlocks()) {
        if (keyBlock->numEntriesInMemBlock > 0) {
            radixSorter->sortSingleKeyBlock(*keyBlock);
            sharedState->appendSortedKeyBlock(keyBlock);
        }
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
