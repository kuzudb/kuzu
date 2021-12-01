#pragma once

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/types.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/processor/include/physical_plan/result/row_collection.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

// This is a shared state between HashJoinBuild and HashJoinProbe operators.
// Each clone of these two operators will share the same state.
// Inside the state, we keep the materialized tuples in rowCollection, which are merged by each
// HashJoinBuild thread when they finished materializing thread-local tuples. Also, the state holds
// a global htDirectory, which will be updated by the last thread in the hash join build side
// task/pipeline, and probed by the HashJoinProbe operators.
class HashJoinSharedState {
public:
    explicit HashJoinSharedState() : htDirectory{nullptr}, hashBitMask{0} {};

    mutex hashJoinSharedStateLock;

    unique_ptr<MemoryBlock> htDirectory;
    unique_ptr<RowCollection> rowCollection;
    uint64_t hashBitMask;
};

struct BuildDataInfo {

public:
    BuildDataInfo(
        const DataPos& keyIDDataPos, vector<DataPos> nonKeyDataPoses, vector<bool> isNonKeyDataFlat)
        : keyIDDataPos{keyIDDataPos}, nonKeyDataPoses{move(nonKeyDataPoses)},
          isNonKeyDataFlat{move(isNonKeyDataFlat)} {}

    BuildDataInfo(const BuildDataInfo& other)
        : BuildDataInfo{other.keyIDDataPos, other.nonKeyDataPoses, other.isNonKeyDataFlat} {}

    inline uint32_t getKeyIDDataChunkPos() const { return keyIDDataPos.dataChunkPos; }

    inline uint32_t getKeyIDVectorPos() const { return keyIDDataPos.valueVectorPos; }

public:
    DataPos keyIDDataPos;
    vector<DataPos> nonKeyDataPoses;
    vector<bool> isNonKeyDataFlat;
};

class HashJoinBuild : public Sink {
public:
    HashJoinBuild(const BuildDataInfo& buildDataInfo, shared_ptr<ResultSet> resultSet,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void init() override;
    void execute() override;
    void finalize() override;

    unique_ptr<PhysicalOperator> clone() override {
        auto clonedResultSet = make_shared<ResultSet>(resultSet->dataChunks.size());
        for (auto i = 0u; i < resultSet->dataChunks.size(); ++i) {
            clonedResultSet->insert(
                i, make_shared<DataChunk>(resultSet->dataChunks[i]->valueVectors.size()));
        }
        auto cloneOp = make_unique<HashJoinBuild>(
            buildDataInfo, move(clonedResultSet), prevOperator->clone(), context, id);
        cloneOp->sharedState = this->sharedState;
        return cloneOp;
    }

    shared_ptr<HashJoinSharedState> sharedState;

private:
    BuildDataInfo buildDataInfo;
    shared_ptr<DataChunk> keyDataChunk;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    unique_ptr<RowCollection> rowCollection;

    void appendResultSet();
};
} // namespace processor
} // namespace graphflow
