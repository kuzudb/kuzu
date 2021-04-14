#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

using namespace std;

namespace graphflow {
namespace processor {

class PhysicalOperatorsInfo {

public:
    void setDataChunkAtPosAsFlat(uint64_t dataChunkPos) {
        dataChunkIsFlatVector[dataChunkPos] = true /* is flat */;
    }

    uint64_t appendAsNewDataChunk(string variableName) {
        vector<string> newDataChunk;
        newDataChunk.push_back(variableName);
        auto newDataChunkPos = vectorVariables.size();
        variableToDataPosMap.insert({variableName, make_pair(newDataChunkPos, 0)});
        vectorVariables.push_back(newDataChunk);
        dataChunkIsFlatVector.push_back(false /* is not flat */);
        return newDataChunkPos;
    }

    // Append the given variable as a new vector into the given dataChunkPos
    void appendAsNewValueVector(string variableName, uint64_t dataChunkPos) {
        auto& dataChunk = vectorVariables[dataChunkPos];
        variableToDataPosMap.insert({variableName, make_pair(dataChunkPos, dataChunk.size())});
        dataChunk.push_back(variableName);
    }

    inline uint64_t getDataChunkPos(string variableName) {
        return variableToDataPosMap.at(variableName).first;
    }

    inline uint64_t getValueVectorPos(string variableName) {
        return variableToDataPosMap.at(variableName).second;
    }

    void clear() {
        dataChunkIsFlatVector.clear();
        vectorVariables.clear();
        variableToDataPosMap.clear();
    }

public:
    // Record isFlat for each data chunk
    vector<bool> dataChunkIsFlatVector;
    // Record variables for each vector, organized as one vector<string> per data chunk.
    vector<vector<string>> vectorVariables;
    // Map each variable to its position pair (dataChunkPos, vectorPos)
    unordered_map<string, pair<uint64_t, uint64_t>> variableToDataPosMap;
};

} // namespace processor
} // namespace graphflow
