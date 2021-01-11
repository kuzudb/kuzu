#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

using namespace std;

namespace graphflow {
namespace processor {

class VarToChunkAndVectorIdxMap {

    typedef unordered_map<string, pair<uint64_t, uint64_t>> variableToDataPosMap_t;

public:
    void put(string variableName, uint64_t dataChunkPos, uint64_t valueVectorPos) {
        variableToDataPosMap.insert({variableName, make_pair(dataChunkPos, valueVectorPos)});
    }

    uint64_t getDataChunkPos(string variableName) {
        return variableToDataPosMap.at(variableName).first;
    }

    uint64_t getValueVectorPos(string variableName) {
        return variableToDataPosMap.at(variableName).second;
    }

private:
    variableToDataPosMap_t variableToDataPosMap;
};

} // namespace processor
} // namespace graphflow
