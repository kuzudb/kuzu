#include "src/processor/include/physical_plan/operator/var_length_join/var_length_join_sink.h"

shared_ptr<ResultSet> VarLengthJoinSink::initResultSet() {
    resultSet = children[0]->initResultSet();
    TableSchema tableSchema;
    for (auto i = 0u; i < inputDataPoses.size(); ++i) {
        auto dataChunkPos = inputDataPoses[i].dataChunkPos;
        auto dataChunk = this->resultSet->dataChunks[dataChunkPos];
        auto vectorPos = inputDataPoses[i].valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        bool isUnflat = !isInputVectorFlat[i];
        tableSchema.appendColumn({isUnflat, dataChunkPos,
            isUnflat ? sizeof(overflow_value_t) : vector->getNumBytesPerValue()});
        vectorsToAppend.push_back(vector);
    }
    localFactorizedTable = make_shared<FactorizedTable>(context.memoryManager, tableSchema);
    return resultSet;
}

void VarLengthJoinSink::execute() {
    metrics->executionTime.start();
    Sink::execute();
    // Append thread-local tuples.
    while (children[0]->getNextTuples()) {
        localFactorizedTable->append(vectorsToAppend);
    }
    varLengthSharedState->appendLocalFactorizedTable(localFactorizedTable);
    metrics->executionTime.stop();
}
