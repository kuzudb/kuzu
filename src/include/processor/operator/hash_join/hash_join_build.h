#pragma once

#include "function/hash/hash_operations.h"
#include "join_hash_table.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class HashJoinBuild;

// This is a shared state between HashJoinBuild and HashJoinProbe operators.
// Each clone of these two operators will share the same state.
// Inside the state, we keep the materialized tuples in factorizedTable, which are merged by each
// HashJoinBuild thread when they finished materializing thread-local tuples. Also, the state holds
// a global htDirectory, which will be updated by the last thread in the hash join build side
// task/pipeline, and probed by the HashJoinProbe operators.
class HashJoinSharedState {
public:
    explicit HashJoinSharedState(std::unique_ptr<JoinHashTable> hashTable)
        : hashTable{std::move(hashTable)} {};

    virtual ~HashJoinSharedState() = default;

    void mergeLocalHashTable(JoinHashTable& localHashTable);

    inline JoinHashTable* getHashTable() { return hashTable.get(); }

protected:
    std::mutex mtx;
    std::unique_ptr<JoinHashTable> hashTable;
};

class HashJoinBuildInfo {
    friend class HashJoinBuild;

public:
    HashJoinBuildInfo(std::vector<DataPos> keysPos, std::vector<DataPos> payloadsPos,
        std::unique_ptr<FactorizedTableSchema> tableSchema)
        : keysPos{std::move(keysPos)}, payloadsPos{std::move(payloadsPos)}, tableSchema{std::move(
                                                                                tableSchema)} {}
    HashJoinBuildInfo(const HashJoinBuildInfo& other)
        : keysPos{other.keysPos}, payloadsPos{other.payloadsPos}, tableSchema{
                                                                      other.tableSchema->copy()} {}

    inline uint32_t getNumKeys() const { return keysPos.size(); }

    inline FactorizedTableSchema* getTableSchema() const { return tableSchema.get(); }

    inline std::unique_ptr<HashJoinBuildInfo> copy() const {
        return std::make_unique<HashJoinBuildInfo>(*this);
    }

private:
    std::vector<DataPos> keysPos;
    std::vector<DataPos> payloadsPos;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
};

class HashJoinBuild : public Sink {
public:
    HashJoinBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashJoinSharedState> sharedState, std::unique_ptr<HashJoinBuildInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : HashJoinBuild{std::move(resultSetDescriptor), PhysicalOperatorType::HASH_JOIN_BUILD,
              std::move(sharedState), std::move(info), std::move(child), id, paramsString} {}
    HashJoinBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        PhysicalOperatorType operatorType, std::shared_ptr<HashJoinSharedState> sharedState,
        std::unique_ptr<HashJoinBuildInfo> info, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), operatorType, std::move(child), id, paramsString},
          sharedState{std::move(sharedState)}, info{std::move(info)} {}
    ~HashJoinBuild() override = default;

    inline std::shared_ptr<HashJoinSharedState> getSharedState() const { return sharedState; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoinBuild>(resultSetDescriptor->copy(), sharedState, info->copy(),
            children[0]->clone(), id, paramsString);
    }

protected:
    virtual void initLocalHashTable(storage::MemoryManager& memoryManager) {
        hashTable = std::make_unique<JoinHashTable>(
            memoryManager, info->getNumKeys(), info->tableSchema->copy());
    }

protected:
    std::shared_ptr<HashJoinSharedState> sharedState;
    std::unique_ptr<HashJoinBuildInfo> info;
    std::vector<common::ValueVector*> vectorsToAppend;
    std::unique_ptr<JoinHashTable> hashTable; // local state
};

} // namespace processor
} // namespace kuzu
