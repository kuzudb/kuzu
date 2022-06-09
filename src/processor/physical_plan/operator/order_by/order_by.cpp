#include "src/processor/include/physical_plan/operator/order_by/order_by.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> OrderBy::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);

    // FactorizedTable, numBytesPerTuple, stringAndUnstructuredKeyColInfo are constructed
    // here because they need the data type information, which is contained in the value vectors.
    TableSchema tableSchema;
    vector<DataType> dataTypes;
    // Loop through all columns to initialize the factorizedTable.
    // We need to store all columns(including keys and payload) in the factorizedTable.
    for (auto i = 0u; i < orderByDataInfo.allDataPoses.size(); ++i) {
        auto dataChunkPos = orderByDataInfo.allDataPoses[i].dataChunkPos;
        auto dataChunk = this->resultSet->dataChunks[dataChunkPos];
        auto vectorPos = orderByDataInfo.allDataPoses[i].valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        bool flattenAllColumnsInFactorizedTable = false;
        // The orderByKeyEncoder requires that the orderByKey columns are flat in the
        // factorizedTable. If there is only one unflat dataChunk, we need to flatten the payload
        // columns in factorizedTable because the payload and key columns are in the same
        // dataChunk.
        if (resultSet->dataChunks.size() == 1) {
            flattenAllColumnsInFactorizedTable = true;
        }
        bool isUnflat = !orderByDataInfo.isVectorFlat[i] && !flattenAllColumnsInFactorizedTable;
        tableSchema.appendColumn({isUnflat, dataChunkPos,
            isUnflat ? (uint32_t)sizeof(overflow_value_t) : vector->getNumBytesPerValue()});
        dataTypes.push_back(vector->dataType);
        vectorsToAppend.push_back(vector);
    }

    // Create a factorizedTable and append it to sharedState.
    localFactorizedTable = make_shared<FactorizedTable>(context->memoryManager, tableSchema);
    factorizedTableIdx = sharedState->getNextFactorizedTableIdx();
    sharedState->appendFactorizedTable(factorizedTableIdx, localFactorizedTable);
    sharedState->setDataTypes(dataTypes);
    // Loop through all key columns and calculate the offsets for string and unstructured columns.
    auto encodedKeyBlockColOffset = 0ul;
    for (auto i = 0u; i < orderByDataInfo.keyDataPoses.size(); i++) {
        auto keyDataPos = orderByDataInfo.keyDataPoses[i];
        auto dataChunkPos = keyDataPos.dataChunkPos;
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        auto vectorPos = keyDataPos.valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        keyVectors.emplace_back(vector);
        if (STRING == vector->dataType.typeID || UNSTRUCTURED == vector->dataType.typeID) {
            // If this is a string or unstructured column, we need to find the
            // factorizedTable offset for this column.
            auto factorizedTableColIdx = 0ul;
            for (auto j = 0u; j < orderByDataInfo.allDataPoses.size(); j++) {
                if (orderByDataInfo.allDataPoses[j] == keyDataPos) {
                    factorizedTableColIdx = j;
                }
            }
            stringAndUnstructuredKeyColInfo.emplace_back(StringAndUnstructuredKeyColInfo(
                localFactorizedTable->getTableSchema().getColOffset(factorizedTableColIdx),
                encodedKeyBlockColOffset, orderByDataInfo.isAscOrder[i],
                STRING == vector->dataType.typeID));
        }
        encodedKeyBlockColOffset += OrderByKeyEncoder::getEncodingSize(vector->dataType);
    }

    // Prepare the orderByEncoder, and radix sorter
    orderByKeyEncoder = make_unique<OrderByKeyEncoder>(keyVectors, orderByDataInfo.isAscOrder,
        context->memoryManager, factorizedTableIdx, localFactorizedTable->getNumTuplesPerBlock());
    radixSorter = make_unique<RadixSort>(context->memoryManager, *localFactorizedTable,
        *orderByKeyEncoder, stringAndUnstructuredKeyColInfo);

    sharedState->setStringAndUnstructuredKeyColInfo(stringAndUnstructuredKeyColInfo);
    sharedState->setNumBytesPerTuple(orderByKeyEncoder->getNumBytesPerTuple());
    return resultSet;
}

void OrderBy::execute(ExecutionContext* context) {
    init(context);
    metrics->executionTime.start();
    // Append thread-local tuples.
    while (children[0]->getNextTuples()) {
        for (auto i = 0u; i < resultSet->multiplicity; i++) {
            orderByKeyEncoder->encodeKeys();
            // The orderByKeyEncoder requires that the orderByKey columns are flat in the
            // factorizedTable. If there is a single dataChunk (unflat or not), it means all the key
            // columns and payload columns are in the same datachunk. Since we need to flatten key
            // columns, we flatten all columns in the factorized table. If there are multiple
            // datachunks, then the datachunks that the keys belong to are guaranteed by the
            // frontend to be flattened (see ProjectionEnumerator), so a column is flat in
            // factorized table if and only if its corresponding vector is flat.
            localFactorizedTable->append(vectorsToAppend);
        }
    }

    for (auto& keyBlock : orderByKeyEncoder->getKeyBlocks()) {
        if (keyBlock->numTuples > 0) {
            radixSorter->sortSingleKeyBlock(*keyBlock);
            sharedState->appendSortedKeyBlock(
                make_shared<MergedKeyBlocks>(orderByKeyEncoder->getNumBytesPerTuple(), keyBlock));
        }
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
