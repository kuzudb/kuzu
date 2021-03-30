#pragma once

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/operations/vector_node_id_operations.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_table.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink/sink.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

class HashJoinSharedState {
public:
    HashJoinSharedState() = default;

    unique_ptr<HashTable> hashTable;
};

class HashJoinBuild : public Sink {
public:
    HashJoinBuild(
        uint64_t keyDataChunkPos, uint64_t keyVectorPos, unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        auto cloneOp =
            make_unique<HashJoinBuild>(keyDataChunkPos, keyVectorPos, prevOperator->clone());
        cloneOp->sharedState = this->sharedState;
        cloneOp->memManager = this->memManager;
        return cloneOp;
    }

    void finalize() override { sharedState->hashTable->buildDirectory(); }

    // The memory manager should be set when processor prepares pipeline tasks
    inline void setMemoryManager(MemoryManager* memManager) {
        this->memManager = memManager;
        sharedState->hashTable->setMemoryManager(memManager);
    }

    shared_ptr<HashJoinSharedState> sharedState;

    unique_ptr<HashTable> initializeHashTable();

private:
    MemoryManager* memManager;
    uint64_t keyDataChunkPos;
    uint64_t keyVectorPos;

    shared_ptr<DataChunk> keyDataChunk;
    shared_ptr<DataChunks> nonKeyDataChunks;
};
} // namespace processor
} // namespace graphflow
