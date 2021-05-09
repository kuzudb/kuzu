#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std;

namespace graphflow {
namespace processor {

class PhysicalOperatorsInfo {

public:
    uint64_t appendAsNewDataChunk(const string& variableName);

    // Append the given variable as a new vector into the given dataChunkPos
    void appendAsNewValueVector(const string& variableName, uint64_t dataChunkPos);

    inline uint64_t getDataChunkPos(const string& variableName) {
        return variableToDataPosMap.at(variableName).first;
    }

    inline uint64_t getValueVectorPos(const string& variableName) {
        return variableToDataPosMap.at(variableName).second;
    }

    void clear();

public:
    // Record isFlat for each data chunk
    vector<bool> dataChunkPosToIsFlat;
    // Record variables for each vector, organized as one vector<string> per data chunk.
    vector<vector<string>> vectorVariables;

private:
    // Map each variable to its position pair (dataChunkPos, vectorPos)
    unordered_map<string, pair<uint64_t, uint64_t>> variableToDataPosMap;
};

} // namespace processor
} // namespace graphflow
