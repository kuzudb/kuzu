#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"

namespace graphflow {
namespace processor {

HashJoinBuild::HashJoinBuild(
    uint64_t keyDataChunkPos, uint64_t keyVectorPos, unique_ptr<PhysicalOperator> prevOperator)
    : Sink(move(prevOperator), HASH_JOIN_BUILD), keyDataChunkPos(keyDataChunkPos),
      keyVectorPos(keyVectorPos) {
    dataChunks = this->prevOperator->getDataChunks();
    keyDataChunk = dataChunks->getDataChunk(keyDataChunkPos);
    nonKeyDataChunks = make_shared<DataChunks>();
    for (uint64_t i = 0; i < dataChunks->getNumDataChunks(); i++) {
        if (i != keyDataChunkPos) {
            nonKeyDataChunks->append(dataChunks->getDataChunk(i));
        }
    }
}

unique_ptr<HashTable> HashJoinBuild::initializeHashTable() {
    vector<PayloadInfo> htPayloadInfos;
    for (uint64_t i = 0; i < keyDataChunk->getNumAttributes(); i++) {
        if (i == keyVectorPos) {
            continue;
        }
        PayloadInfo info(keyDataChunk->getValueVector(i)->getNumBytesPerValue(), false);
        htPayloadInfos.push_back(info);
    }
    for (uint64_t i = 0; i < nonKeyDataChunks->getNumDataChunks(); i++) {
        auto dataChunk = nonKeyDataChunks->getDataChunk(i);
        for (auto& vector : dataChunk->valueVectors) {
            PayloadInfo info(
                (dataChunk->isFlat() ? vector->getNumBytesPerValue() : sizeof(overflow_value_t)),
                !dataChunk->isFlat());
            htPayloadInfos.push_back(info);
        }
    }
    return make_unique<HashTable>(memManager, htPayloadInfos);
}

void HashJoinBuild::getNextTuples() {
    while (prevOperator->hasNextMorsel()) {
        prevOperator->getNextTuples();
        if (0 == keyDataChunk->numSelectedValues) {
            break;
        }
        sharedState->hashTable->addDataChunks(*keyDataChunk, keyVectorPos, *nonKeyDataChunks);
    }
    keyDataChunk->size = 0;
    keyDataChunk->numSelectedValues = 0;
}
} // namespace processor
} // namespace graphflow
