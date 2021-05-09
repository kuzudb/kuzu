#include "src/processor/include/physical_plan/operator/physical_operator_info.h"

namespace graphflow {
namespace processor {

uint64_t PhysicalOperatorsInfo::appendAsNewDataChunk(const string& variableName) {
    vector<string> newDataChunk;
    newDataChunk.push_back(variableName);
    auto newDataChunkPos = vectorVariables.size();
    variableToDataPosMap.insert({variableName, make_pair(newDataChunkPos, 0)});
    vectorVariables.push_back(newDataChunk);
    dataChunkPosToIsFlat.push_back(false /* is not flat */);
    return newDataChunkPos;
}

void PhysicalOperatorsInfo::appendAsNewValueVector(
    const string& variableName, uint64_t dataChunkPos) {
    auto& dataChunk = vectorVariables[dataChunkPos];
    variableToDataPosMap.insert({variableName, make_pair(dataChunkPos, dataChunk.size())});
    dataChunk.push_back(variableName);
}

void PhysicalOperatorsInfo::clear() {
    dataChunkPosToIsFlat.clear();
    vectorVariables.clear();
    variableToDataPosMap.clear();
}

} // namespace processor
} // namespace graphflow
