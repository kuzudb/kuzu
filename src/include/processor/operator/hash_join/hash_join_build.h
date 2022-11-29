#pragma once

#include "function/hash/hash_operations.h"
#include "join_hash_table.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"

using namespace kuzu::function::operation;

namespace kuzu {
namespace processor {

// This is a shared state between HashJoinBuild and HashJoinProbe operators.
// Each clone of these two operators will share the same state.
// Inside the state, we keep the materialized tuples in factorizedTable, which are merged by each
// HashJoinBuild thread when they finished materializing thread-local tuples. Also, the state holds
// a global htDirectory, which will be updated by the last thread in the hash join build side
// task/pipeline, and probed by the HashJoinProbe operators.
class HashJoinSharedState {
public:
    HashJoinSharedState() = default;

    virtual ~HashJoinSharedState() = default;

    virtual void initEmptyHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
        unique_ptr<FactorizedTableSchema> tableSchema);

    void mergeLocalHashTable(JoinHashTable& localHashTable);

    inline JoinHashTable* getHashTable() { return hashTable.get(); }

protected:
    mutex mtx;
    unique_ptr<JoinHashTable> hashTable;
};

struct BuildDataInfo {
public:
    BuildDataInfo(vector<pair<DataPos, DataType>> keysPosAndType,
        vector<pair<DataPos, DataType>> payloadsPosAndType, vector<bool> isPayloadsFlat,
        vector<bool> isPayloadsInKeyChunk)
        : keysPosAndType{std::move(keysPosAndType)}, payloadsPosAndType{std::move(
                                                         payloadsPosAndType)},
          isPayloadsFlat{std::move(isPayloadsFlat)}, isPayloadsInKeyChunk{
                                                         std::move(isPayloadsInKeyChunk)} {}

    BuildDataInfo(const BuildDataInfo& other)
        : BuildDataInfo{other.keysPosAndType, other.payloadsPosAndType, other.isPayloadsFlat,
              other.isPayloadsInKeyChunk} {}

    inline uint32_t getNumKeys() const { return keysPosAndType.size(); }

public:
    vector<pair<DataPos, DataType>> keysPosAndType;
    vector<pair<DataPos, DataType>> payloadsPosAndType;
    vector<bool> isPayloadsFlat;
    vector<bool> isPayloadsInKeyChunk;
};

class HashJoinBuild : public Sink {
public:
    HashJoinBuild(shared_ptr<HashJoinSharedState> sharedState, const BuildDataInfo& buildDataInfo,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : Sink{std::move(child), id, paramsString}, sharedState{std::move(sharedState)},
          buildDataInfo{buildDataInfo} {}
    ~HashJoinBuild() override = default;

    inline PhysicalOperatorType getOperatorType() override { return HASH_JOIN_BUILD; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalize(ExecutionContext* context) override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoinBuild>(
            sharedState, buildDataInfo, children[0]->clone(), id, paramsString);
    }

protected:
    // TODO(Guodong/Xiyang): construct schema in mapper.
    unique_ptr<FactorizedTableSchema> populateTableSchema();
    void initGlobalStateInternal(ExecutionContext* context) override;

    virtual void initLocalHashTable(
        MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema);
    inline void appendVectors() { hashTable->append(vectorsToAppend); }

protected:
    shared_ptr<HashJoinSharedState> sharedState;
    BuildDataInfo buildDataInfo;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    unique_ptr<JoinHashTable> hashTable;
};

} // namespace processor
} // namespace kuzu
