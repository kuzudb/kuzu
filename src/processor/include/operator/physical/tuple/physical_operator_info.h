#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "src/storage/include/structures/common.h"

using namespace std;
using namespace graphflow::storage;

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

    void putListSyncer(uint64_t dataChunkPos, shared_ptr<ListSyncer> listSyncer) {
        listSyncerPerDataChunk.insert({dataChunkPos, listSyncer});
    }

    shared_ptr<ListSyncer> getListSyncer(uint64_t dataChunkPos) {
        return listSyncerPerDataChunk[dataChunkPos];
    }

    uint64_t getDataChunkPos(string variableName) {
        return variableToDataPosMap.at(variableName).first;
    }

    uint64_t getValueVectorPos(string variableName) {
        return variableToDataPosMap.at(variableName).second;
    }

private:
    unordered_map<string, pair<uint64_t, uint64_t>> variableToDataPosMap;
    unordered_map<uint64_t, shared_ptr<ListSyncer>> listSyncerPerDataChunk;
    unordered_map<uint64_t, bool> dataChunkPosToIsFlatMap;
};

} // namespace processor
} // namespace graphflow
