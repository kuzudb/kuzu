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
    void put(string variableName, uint64_t dataChunkPos, uint64_t valueVectorPos) {
        variableToDataPosMap.insert({variableName, make_pair(dataChunkPos, valueVectorPos)});
        dataChunkPosToIsFlatMap.insert({dataChunkPos, false /* is not flat */});
    }

    void setDataChunkAtPosAsFlat(uint64_t dataChunkPos) {
        dataChunkPosToIsFlatMap.find(dataChunkPos)->second = true /* is flat */;
    }

    bool isFlat(uint64_t dataChunkPos) {
        return dataChunkPosToIsFlatMap.find(dataChunkPos)->second;
    }

    uint64_t getDataChunkPos(string variableName) {
        return variableToDataPosMap.at(variableName).first;
    }

    uint64_t getValueVectorPos(string variableName) {
        return variableToDataPosMap.at(variableName).second;
    }

private:
    unordered_map<string, pair<uint64_t, uint64_t>> variableToDataPosMap;
    unordered_map<uint64_t, bool> dataChunkPosToIsFlatMap;
};

} // namespace processor
} // namespace graphflow
