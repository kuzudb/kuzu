#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/hash_join/hash_table.h"

using namespace graphflow::processor;
using namespace std;

constexpr const int64_t NUM_BYTES_FOR_NODE_OFFSET = sizeof(node_offset_t);
constexpr const int64_t NUM_BYTES_FOR_NODE_LABEL = sizeof(label_t);

struct unpadded_result_t {
    int64_t keyOffset : 64;
    int32_t keyLabel : 32;
    int32_t age : 32;
    int64_t payloadOffset : 64;
    int32_t num : 32;
};

void generateSingleLabelDataChunks(
    shared_ptr<DataChunk> keyDataChunk, DataChunks& payloadDataChunks) {
    auto keyIdVec = make_shared<NodeIDSequenceVector>(8);
    keyIdVec->setStartOffset(0);
    auto ageVec = make_shared<ValueVector>(INT32);
    auto ageVecData = (int32_t*)ageVec->getValues();
    for (uint64_t i = 0; i < ValueVector::DEFAULT_VECTOR_SIZE; i++) {
        ageVecData[i] = (i + 20) % 100;
    }
    keyDataChunk->append(keyIdVec);
    keyDataChunk->append(ageVec);
    keyIdVec->setDataChunkOwner(keyDataChunk);
    ageVec->setDataChunkOwner(keyDataChunk);
    keyDataChunk->size = ValueVector::DEFAULT_VECTOR_SIZE;

    auto payloadDataChunk = make_shared<DataChunk>();
    NodeIDCompressionScheme compressionScheme;
    auto payloadIdVector = make_shared<NodeIDVector>(compressionScheme);
    payloadIdVector->setCommonLabel(100);
    auto payloadNumVector = make_shared<ValueVector>(INT32);
    auto payloadIdVectorPtr = (int64_t*)payloadIdVector->getValues();
    auto buildNumVectorPtr = (int32_t*)payloadNumVector->getValues();
    for (uint64_t i = 0; i < ValueVector::DEFAULT_VECTOR_SIZE; i++) {
        payloadIdVectorPtr[i] = i * 2 + 1;
        buildNumVectorPtr[i] = i + 100;
    }
    payloadDataChunk->append(payloadIdVector);
    payloadIdVector->setDataChunkOwner(payloadDataChunk);
    payloadDataChunk->append(payloadNumVector);
    payloadNumVector->setDataChunkOwner(payloadDataChunk);
    payloadDataChunk->size = ValueVector::DEFAULT_VECTOR_SIZE;
    payloadDataChunks.append(payloadDataChunk);
}

TEST(HashTableTests, HashTableJoinOnListKey) {
    vector<PayloadInfo> payloadInfos;
    payloadInfos.emplace_back(4, false);
    payloadInfos.emplace_back(8, false);
    payloadInfos.emplace_back(4, false);

    MemoryManager memMgr(1 << 30);
    auto hashTable = make_unique<HashTable>(memMgr, payloadInfos);

    auto keyChunk = make_shared<DataChunk>();
    DataChunks payloadDataChunks;
    generateSingleLabelDataChunks(keyChunk, payloadDataChunks);

    keyChunk->currPos = -1;
    auto payloadDataChunk = payloadDataChunks.getDataChunk(0);
    payloadDataChunk->currPos = 1;
    hashTable->addDataChunks(*keyChunk, 0, payloadDataChunks);
    payloadDataChunk->currPos = 2;
    hashTable->addDataChunks(*keyChunk, 0, payloadDataChunks);

    hashTable->buildDirectory();

    DataChunks probeDataChunks;
    auto probeChunk = make_shared<DataChunk>();
    NodeIDCompressionScheme nodeIDScheme;
    auto probeNodeID = make_shared<NodeIDVector>(8, nodeIDScheme);
    auto nodeIDVectorPtr = (int64_t*)probeNodeID->getValues();
    for (int32_t i = 0; i < 20; i++) {
        nodeIDVectorPtr[i] = i % 8;
    }
    probeChunk->append(probeNodeID);
    probeNodeID->setDataChunkOwner(probeChunk);
    probeChunk->size = 20;
    probeChunk->currPos = -1;
    probeDataChunks.append(probeChunk);

    unique_ptr<uint8_t*[]> probedTuples = make_unique<uint8_t*[]>(ValueVector::MAX_VECTOR_SIZE);
    auto numProbedTuples = probeNodeID->size();
    hashTable->probeDirectory(*probeNodeID, probedTuples.get());

    int64_t matchCount = 0;
    int64_t currentProbeCount = numProbedTuples;
    unpadded_result_t result;
    while (currentProbeCount > 0) {
        currentProbeCount = 0;
        for (uint64_t i = 0; i < numProbedTuples; i++) {
            if (probedTuples[i]) {
                memcpy(&result, (uint8_t*)probedTuples[i], sizeof(unpadded_result_t));
                auto expectedKyeOffset = i % 8;
                ASSERT_EQ(result.keyOffset, expectedKyeOffset);
                ASSERT_EQ(result.keyLabel, 8);
                ASSERT_EQ(result.age, (expectedKyeOffset + 20) % 100);
                ASSERT_TRUE(result.payloadOffset == 3 || result.payloadOffset == 5);
                ASSERT_TRUE(result.num == 101 || result.num == 102);
                matchCount++;
                auto next = (uint8_t**)(probedTuples[i] + NUM_BYTES_PER_NODE_ID + sizeof(int32_t) +
                                        NUM_BYTES_FOR_NODE_OFFSET + sizeof(int32_t));
                memcpy(&probedTuples[i], (uint8_t*)next, sizeof(uint8_t*));
                if (probedTuples[i]) {
                    currentProbeCount++;
                }
            }
        }
    }
    ASSERT_EQ(matchCount, 40);
}

TEST(HashTableTests, HashTableJoinOnFlatKey) {
    vector<PayloadInfo> payloadInfos;
    payloadInfos.emplace_back(4, false);
    payloadInfos.emplace_back(8, true);
    payloadInfos.emplace_back(4, true);

    MemoryManager memMgr(1 << 30);
    auto hashTable = make_unique<HashTable>(memMgr, payloadInfos);

    auto keyChunk = make_shared<DataChunk>();
    DataChunks payloadDataChunks;
    generateSingleLabelDataChunks(keyChunk, payloadDataChunks);

    auto payloadDataChunk = payloadDataChunks.getDataChunk(0);
    payloadDataChunk->currPos = -1;

    keyChunk->currPos = 1;
    hashTable->addDataChunks(*keyChunk, 0, payloadDataChunks);
    keyChunk->currPos = 2;
    hashTable->addDataChunks(*keyChunk, 0, payloadDataChunks);
    hashTable->buildDirectory();

    // probeChunks contain one data chunk: [nodeID]
    DataChunks probeDataChunks;
    auto probeChunk = make_shared<DataChunk>();
    NodeIDCompressionScheme nodeIDScheme;
    auto probeNodeID = make_shared<NodeIDVector>(8, nodeIDScheme);
    auto nodeIDVectorPtr = (int64_t*)probeNodeID->getValues();
    for (int32_t i = 0; i < 10; i++) {
        nodeIDVectorPtr[i] = i % 8;
    }
    probeChunk->append(probeNodeID);
    probeNodeID->setDataChunkOwner(probeChunk);
    probeChunk->size = 10;
    probeChunk->currPos = -1;
    probeDataChunks.append(probeChunk);

    unique_ptr<uint8_t*[]> probedTuples = make_unique<uint8_t*[]>(ValueVector::MAX_VECTOR_SIZE);
    auto numProbedTuples = probeNodeID->size();
    hashTable->probeDirectory(*probeNodeID, probedTuples.get());

    auto matchCount = 0;
    int64_t currentProbeCount = numProbedTuples;
    node_offset_t keyOffset;
    label_t keyLabel;
    int32_t age;
    int32_t expected_num_array[ValueVector::DEFAULT_VECTOR_SIZE];
    for (uint64_t i = 0; i < ValueVector::DEFAULT_VECTOR_SIZE; i++) {
        expected_num_array[i] = i + 100;
    }
    int64_t expected_offset_array[ValueVector::DEFAULT_VECTOR_SIZE];
    for (uint64_t i = 0; i < ValueVector::DEFAULT_VECTOR_SIZE; i++) {
        expected_offset_array[i] = i * 2 + 1;
    }
    while (currentProbeCount > 0) {
        currentProbeCount = 0;
        for (uint64_t i = 0; i < numProbedTuples; i++) {
            if (probedTuples[i]) {
                memcpy(&keyOffset, (uint8_t*)probedTuples[i], NUM_BYTES_FOR_NODE_OFFSET);
                memcpy(&keyLabel, (uint8_t*)probedTuples[i] + NUM_BYTES_FOR_NODE_OFFSET,
                    NUM_BYTES_FOR_NODE_LABEL);
                memcpy(&age, (uint8_t*)probedTuples[i] + NUM_BYTES_PER_NODE_ID, sizeof(int32_t));
                overflow_value_t payloadOffsetOverflow, numOverflow;
                memcpy(&payloadOffsetOverflow,
                    (uint8_t*)probedTuples[i] + NUM_BYTES_PER_NODE_ID + sizeof(int32_t),
                    sizeof(overflow_value_t));
                memcpy(&numOverflow,
                    (uint8_t*)probedTuples[i] + NUM_BYTES_PER_NODE_ID + sizeof(int32_t) +
                        sizeof(overflow_value_t),
                    sizeof(overflow_value_t));
                auto expectedKyeOffset = i % 8;
                ASSERT_EQ(keyOffset, expectedKyeOffset);
                ASSERT_EQ(age, (expectedKyeOffset + 20) % 100);
                ASSERT_TRUE(0 == memcmp(payloadOffsetOverflow.value, &expected_offset_array,
                                     payloadOffsetOverflow.len));
                ASSERT_TRUE(0 == memcmp(numOverflow.value, &expected_num_array, numOverflow.len));
                auto next = (uint8_t**)(probedTuples[i] + NUM_BYTES_PER_NODE_ID + sizeof(int32_t) +
                                        sizeof(overflow_value_t) + sizeof(overflow_value_t));
                memcpy(&probedTuples[i], (uint8_t*)next, sizeof(uint8_t*));
                if (probedTuples[i]) {
                    currentProbeCount++;
                }
                matchCount++;
            }
        }
    }
    ASSERT_EQ(matchCount, 3);
}

TEST(HashTableTests, HashTableJoinOnListKeyLarge) {
    vector<PayloadInfo> payloadInfos;
    payloadInfos.emplace_back(4, false);
    payloadInfos.emplace_back(8, false);
    payloadInfos.emplace_back(4, false);

    MemoryManager memMgr(1 << 30);
    auto hashTable = make_unique<HashTable>(memMgr, payloadInfos);

    auto keyChunk = make_shared<DataChunk>();
    DataChunks payloadDataChunks;
    generateSingleLabelDataChunks(keyChunk, payloadDataChunks);

    keyChunk->currPos = -1;
    auto payloadDataChunk = payloadDataChunks.getDataChunk(0);
    auto keyVec = static_pointer_cast<NodeIDSequenceVector>(keyChunk->getValueVector(0));
    for (uint64_t i = 0; i < ValueVector::DEFAULT_VECTOR_SIZE; i++) {
        payloadDataChunk->currPos = i;
        keyVec->setStartOffset(i * ValueVector::DEFAULT_VECTOR_SIZE);
        hashTable->addDataChunks(*keyChunk, 0, payloadDataChunks);
    }
    hashTable->buildDirectory();

    DataChunks probeDataChunks;
    auto probeChunk = make_shared<DataChunk>();
    NodeIDCompressionScheme nodeIDScheme;
    auto probeNodeID = make_shared<NodeIDVector>(8, nodeIDScheme);
    auto nodeIDVectorPtr = (int64_t*)probeNodeID->getValues();
    for (uint64_t i = 0; i < 10; i++) {
        nodeIDVectorPtr[i] = i * ValueVector::DEFAULT_VECTOR_SIZE + 15;
    }
    probeChunk->append(probeNodeID);
    probeNodeID->setDataChunkOwner(probeChunk);
    probeChunk->size = 10;
    probeChunk->currPos = -1;
    probeDataChunks.append(probeChunk);

    unique_ptr<uint8_t*[]> probedTuples = make_unique<uint8_t*[]>(ValueVector::MAX_VECTOR_SIZE);
    auto numProbedTuples = probeNodeID->size();
    hashTable->probeDirectory(*probeNodeID, probedTuples.get());

    int64_t matchCount = 0;
    int64_t currentProbeCount = numProbedTuples;
    unpadded_result_t result;
    while (currentProbeCount > 0) {
        currentProbeCount = 0;
        for (uint64_t i = 0; i < numProbedTuples; i++) {
            if (probedTuples[i]) {
                memcpy(&result, (uint8_t*)probedTuples[i], sizeof(unpadded_result_t));
                auto expectedKyeOffset = i * ValueVector::DEFAULT_VECTOR_SIZE + 15;
                ASSERT_EQ(result.keyOffset, expectedKyeOffset);
                ASSERT_EQ(result.keyLabel, 8);
                ASSERT_EQ(result.age, 35);
                ASSERT_EQ(result.payloadOffset, (2 * i) + 1);
                ASSERT_EQ(result.num, 100 + i);
                matchCount++;
                auto next = (uint8_t**)(probedTuples[i] + NUM_BYTES_PER_NODE_ID + sizeof(int32_t) +
                                        NUM_BYTES_FOR_NODE_OFFSET + sizeof(int32_t));
                memcpy(&probedTuples[i], (uint8_t*)next, sizeof(uint8_t*));
                if (probedTuples[i]) {
                    currentProbeCount++;
                }
            }
        }
    }
    ASSERT_EQ(matchCount, 10);
}

TEST(HashTableTests, HashTableWithMultiLabelKey) {
    vector<PayloadInfo> payloadInfos;
    payloadInfos.emplace_back(4, false);
    payloadInfos.emplace_back(8, false);
    payloadInfos.emplace_back(4, false);

    MemoryManager memMgr(1 << 30);
    auto hashTable = make_unique<HashTable>(memMgr, payloadInfos);

    auto keyChunk = make_shared<DataChunk>();
    DataChunks payloadDataChunks;
    generateSingleLabelDataChunks(keyChunk, payloadDataChunks);

    auto payloadDataChunk = payloadDataChunks.getDataChunk(0);
    payloadDataChunk->currPos = 1;

    auto keyVec = static_pointer_cast<NodeIDVector>(keyChunk->getValueVector(0));
    keyChunk->currPos = 1;
    keyVec->setCommonLabel(8);
    hashTable->addDataChunks(*keyChunk, 0, payloadDataChunks);
    keyChunk->currPos = 2;
    keyVec->setCommonLabel(16);
    hashTable->addDataChunks(*keyChunk, 0, payloadDataChunks);
    hashTable->buildDirectory();

    // probeChunks contain one data chunk: [nodeID]
    DataChunks probeDataChunks;
    auto probeChunk = make_shared<DataChunk>();
    NodeIDCompressionScheme nodeIDScheme;
    auto probeNodeID = make_shared<NodeIDVector>(8, nodeIDScheme);
    auto nodeIDVectorPtr = (int64_t*)probeNodeID->getValues();
    for (int32_t i = 0; i < 10; i++) {
        nodeIDVectorPtr[i] = i % 8;
    }
    probeChunk->append(probeNodeID);
    probeNodeID->setDataChunkOwner(probeChunk);
    probeChunk->size = 10;
    probeChunk->currPos = -1;
    probeDataChunks.append(probeChunk);

    unique_ptr<uint8_t*[]> probedTuples = make_unique<uint8_t*[]>(ValueVector::MAX_VECTOR_SIZE);
    auto numProbedTuples = probeNodeID->size();
    hashTable->probeDirectory(*probeNodeID, probedTuples.get());

    int64_t matchCount = 0;
    int64_t currentProbeCount = numProbedTuples;
    unpadded_result_t result;
    while (currentProbeCount > 0) {
        currentProbeCount = 0;
        for (uint64_t i = 0; i < numProbedTuples; i++) {
            if (probedTuples[i]) {
                memcpy(&result, (uint8_t*)probedTuples[i], sizeof(unpadded_result_t));
                auto expectedKyeOffset = i % 8;
                ASSERT_EQ(result.keyOffset, expectedKyeOffset);
                ASSERT_EQ(result.keyLabel, 8 * expectedKyeOffset);
                ASSERT_EQ(result.age, (expectedKyeOffset + 20) % 100);
                ASSERT_TRUE(result.payloadOffset == 3);
                ASSERT_TRUE(result.num == 101);
                matchCount++;
                auto next = (uint8_t**)(probedTuples[i] + NUM_BYTES_PER_NODE_ID + sizeof(int32_t) +
                                        NUM_BYTES_FOR_NODE_OFFSET + sizeof(int32_t));
                memcpy(&probedTuples[i], (uint8_t*)next, sizeof(uint8_t*));
                if (probedTuples[i]) {
                    currentProbeCount++;
                }
            }
        }
    }
    ASSERT_EQ(matchCount, 2);
}
