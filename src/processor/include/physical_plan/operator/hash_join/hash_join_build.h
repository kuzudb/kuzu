#pragma once

#include "src/function/hash/operations/include/hash_operations.h"
#include "src/processor/include/physical_plan/hash_table/join_hash_table.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"
#include "src/processor/include/physical_plan/result/result_set.h"

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
    explicit HashJoinSharedState(vector<DataType> payloadDataTypes)
        : payloadDataTypes{move(payloadDataTypes)} {}

    virtual ~HashJoinSharedState() = default;

    virtual void initEmptyHashTableIfNecessary(MemoryManager& memoryManager, uint64_t numKeyColumns,
        unique_ptr<FactorizedTableSchema> tableSchema);

    void mergeLocalHashTable(JoinHashTable& localHashTable);

    inline JoinHashTable* getHashTable() { return hashTable.get(); }

    inline vector<DataType> getPayloadDataTypes() { return payloadDataTypes; }

protected:
    mutex hashJoinSharedStateMutex;
    unique_ptr<JoinHashTable> hashTable;
    vector<DataType> payloadDataTypes;
};

struct BuildDataInfo {

public:
    BuildDataInfo(vector<DataPos> keysDataPos, vector<DataPos> payloadsDataPos,
        vector<bool> isPayloadsFlat, vector<bool> isPayloadsInKeyChunk)
        : keysDataPos{std::move(keysDataPos)}, payloadsDataPos{std::move(payloadsDataPos)},
          isPayloadsFlat{move(isPayloadsFlat)}, isPayloadsInKeyChunk{move(isPayloadsInKeyChunk)} {}

    BuildDataInfo(const BuildDataInfo& other)
        : BuildDataInfo{other.keysDataPos, other.payloadsDataPos, other.isPayloadsFlat,
              other.isPayloadsInKeyChunk} {}

public:
    vector<DataPos> keysDataPos;
    vector<DataPos> payloadsDataPos;
    vector<bool> isPayloadsFlat;
    vector<bool> isPayloadsInKeyChunk;
};

class HashJoinBuild : public Sink {
public:
    HashJoinBuild(shared_ptr<HashJoinSharedState> sharedState, const BuildDataInfo& buildDataInfo,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : Sink{move(child), id, paramsString}, sharedState{move(sharedState)}, buildDataInfo{
                                                                                   buildDataInfo} {}
    ~HashJoinBuild() override = default;

    inline PhysicalOperatorType getOperatorType() override { return HASH_JOIN_BUILD; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void execute(ExecutionContext* context) override;
    void finalize(ExecutionContext* context) override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoinBuild>(
            sharedState, buildDataInfo, children[0]->clone(), id, paramsString);
    }

protected:
    virtual void initHashTable(
        MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema);
    inline void appendVectors() { hashTable->append(vectorsToAppend); }

protected:
    shared_ptr<HashJoinSharedState> sharedState;
    BuildDataInfo buildDataInfo;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    unique_ptr<JoinHashTable> hashTable;
};

} // namespace processor
} // namespace graphflow
