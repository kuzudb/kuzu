#pragma once

#include "src/function/hash/operations/include/hash_operations.h"
#include "src/processor/include/physical_plan/hash_table/join_hash_table.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace std;
using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

// This is a shared state between HashJoinBuild and HashJoinProbe operators.
// Each clone of these two operators will share the same state.
// Inside the state, we keep the materialized tuples in factorizedTable, which are merged by each
// HashJoinBuild thread when they finished materializing thread-local tuples. Also, the state holds
// a global htDirectory, which will be updated by the last thread in the hash join build side
// task/pipeline, and probed by the HashJoinProbe operators.
class HashJoinSharedState {
public:
    explicit HashJoinSharedState(vector<DataType> nonKeyDataPosesDataTypes)
        : nonKeyDataPosesDataTypes{move(nonKeyDataPosesDataTypes)} {}

    void initEmptyHashTableIfNecessary(
        MemoryManager& memoryManager, unique_ptr<TableSchema> tableSchema);

    void mergeLocalHashTable(JoinHashTable& localHashTable);

    inline JoinHashTable* getHashTable() { return hashTable.get(); }

    inline vector<DataType> getNonKeyDataPosesDataTypes() { return nonKeyDataPosesDataTypes; }

private:
    mutex hashJoinSharedStateMutex;
    unique_ptr<JoinHashTable> hashTable;
    vector<DataType> nonKeyDataPosesDataTypes;
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
    HashJoinBuild(shared_ptr<HashJoinSharedState> sharedState, const BuildDataInfo& buildDataInfo,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : Sink{move(child), id, paramsString}, sharedState{move(sharedState)}, buildDataInfo{
                                                                                   buildDataInfo} {}

    PhysicalOperatorType getOperatorType() override { return HASH_JOIN_BUILD; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void execute(ExecutionContext* context) override;
    void finalize(ExecutionContext* context) override;

    inline HashJoinSharedState* getJoinSharedState() { return sharedState.get(); }
    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoinBuild>(
            sharedState, buildDataInfo, children[0]->clone(), id, paramsString);
    }

private:
    void appendVectors();

private:
    shared_ptr<HashJoinSharedState> sharedState;

    BuildDataInfo buildDataInfo;
    shared_ptr<DataChunk> keyDataChunk;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    unique_ptr<JoinHashTable> hashTable;
};

} // namespace processor
} // namespace graphflow
