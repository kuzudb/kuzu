#pragma once

#include "aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"

namespace kuzu {
namespace processor {

// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor): This is a final class.
class HashAggregateSharedState final : public BaseAggregateSharedState {

public:
    explicit HashAggregateSharedState(
        const std::vector<function::AggregateFunction>& aggregateFunctions)
        : BaseAggregateSharedState{aggregateFunctions}, limitCounter{0},
          limitNumber{common::INVALID_LIMIT} {}

    void appendAggregateHashTable(std::unique_ptr<AggregateHashTable> aggregateHashTable);

    void combineAggregateHashTable(storage::MemoryManager& memoryManager);

    void finalizeAggregateHashTable();

    std::pair<uint64_t, uint64_t> getNextRangeToRead() override;

    inline uint8_t* getRow(uint64_t idx) { return globalAggregateHashTable->getEntry(idx); }

    FactorizedTable* getFactorizedTable() { return globalAggregateHashTable->getFactorizedTable(); }

    uint64_t getCurrentOffset() const { return currentOffset; }

    // return whether limitNumber is exceeded
    bool increaseAndCheckLimitCount(uint64_t num);

    void setLimitNumber(uint64_t num) { limitNumber = num; }

private:
    std::vector<std::unique_ptr<AggregateHashTable>> localAggregateHashTables;
    std::unique_ptr<AggregateHashTable> globalAggregateHashTable;
    std::atomic_uint64_t limitCounter;
    uint64_t limitNumber;
};

struct HashAggregateInfo {
    std::vector<DataPos> flatKeysPos;
    std::vector<DataPos> unFlatKeysPos;
    std::vector<DataPos> dependentKeysPos;
    FactorizedTableSchema tableSchema;

    HashAggregateInfo(std::vector<DataPos> flatKeysPos, std::vector<DataPos> unFlatKeysPos,
        std::vector<DataPos> dependentKeysPos, FactorizedTableSchema tableSchema);
    HashAggregateInfo(const HashAggregateInfo& other);
};

struct HashAggregateLocalState {
    std::vector<common::ValueVector*> flatKeyVectors;
    std::vector<common::ValueVector*> unFlatKeyVectors;
    std::vector<common::ValueVector*> dependentKeyVectors;
    common::DataChunkState* leadingState = nullptr;
    std::unique_ptr<AggregateHashTable> aggregateHashTable;

    void init(ResultSet& resultSet, main::ClientContext* context, HashAggregateInfo& info,
        std::vector<function::AggregateFunction>& aggregateFunctions,
        std::vector<common::LogicalType> types);
    uint64_t append(const std::vector<AggregateInput>& aggregateInputs,
        uint64_t multiplicity) const;
};

struct HashAggregatePrintInfo final : OPPrintInfo {
    binder::expression_vector keys;
    binder::expression_vector aggregates;
    uint64_t limitNum;

    HashAggregatePrintInfo(binder::expression_vector keys, binder::expression_vector aggregates)
        : keys{std::move(keys)}, aggregates{std::move(aggregates)}, limitNum{UINT64_MAX} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<HashAggregatePrintInfo>(new HashAggregatePrintInfo(*this));
    }

private:
    HashAggregatePrintInfo(const HashAggregatePrintInfo& other)
        : OPPrintInfo{other}, keys{other.keys}, aggregates{other.aggregates},
          limitNum{other.limitNum} {}
};

class HashAggregate : public BaseAggregate {
public:
    HashAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashAggregateSharedState> sharedState, HashAggregateInfo hashInfo,
        std::vector<function::AggregateFunction> aggregateFunctions,
        std::vector<AggregateInfo> aggInfos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateFunctions),
              std::move(aggInfos), std::move(child), id, std::move(printInfo)},
          hashInfo{std::move(hashInfo)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalizeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashAggregate>(resultSetDescriptor->copy(), sharedState, hashInfo,
            copyVector(aggregateFunctions), copyVector(aggInfos), children[0]->clone(), id,
            printInfo->copy());
    }

    std::shared_ptr<HashAggregateSharedState> getSharedState() const { return sharedState; }

private:
    HashAggregateInfo hashInfo;
    HashAggregateLocalState localState;
    std::shared_ptr<HashAggregateSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
