#pragma once

#include "insert_executor.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/pattern_creation_info_table.h"
#include "set_executor.h"

namespace kuzu {
namespace processor {

struct MergeInfo {
    std::vector<DataPos> keyPoses;
    FactorizedTableSchema tableSchema;
    common::executor_info executorInfo;
    DataPos existenceMark;

    MergeInfo(std::vector<DataPos> keyPoses, FactorizedTableSchema tableSchema,
        common::executor_info executorInfo, DataPos existenceMark)
        : keyPoses{std::move(keyPoses)}, tableSchema{std::move(tableSchema)},
          executorInfo{std::move(executorInfo)}, existenceMark{std::move(existenceMark)} {}

    MergeInfo copy() const {
        return MergeInfo{keyPoses, tableSchema.copy(), executorInfo, existenceMark};
    }
};

struct MergePrintInfo final : OPPrintInfo {
    binder::expression_vector pattern;
    std::vector<binder::expression_pair> onCreate;
    std::vector<binder::expression_pair> onMatch;

    MergePrintInfo(binder::expression_vector pattern, std::vector<binder::expression_pair> onCreate,
        std::vector<binder::expression_pair> onMatch)
        : pattern(std::move(pattern)), onCreate(std::move(onCreate)), onMatch(std::move(onMatch)) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<MergePrintInfo>(new MergePrintInfo(*this));
    }

private:
    MergePrintInfo(const MergePrintInfo& other)
        : OPPrintInfo(other), pattern(other.pattern), onCreate(other.onCreate),
          onMatch(other.onMatch) {}
};

struct MergeLocalState {
    std::vector<common::ValueVector*> keyVectors;
    std::unique_ptr<PatternCreationInfoTable> hashTable;
    common::ValueVector* existenceVector = nullptr;

    void init(ResultSet& resultSet, main::ClientContext* context, MergeInfo& info);

    bool patternExists() const;

    PatternCreationInfo getPatternCreationInfo() const {
        return hashTable->getPatternCreationInfo(keyVectors);
    }
};

class Merge : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::MERGE;

public:
    Merge(std::vector<NodeInsertExecutor> nodeInsertExecutors,
        std::vector<RelInsertExecutor> relInsertExecutors,
        std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors,
        std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors,
        std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors,
        std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors, MergeInfo info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          nodeInsertExecutors{std::move(nodeInsertExecutors)},
          relInsertExecutors{std::move(relInsertExecutors)},
          onCreateNodeSetExecutors{std::move(onCreateNodeSetExecutors)},
          onCreateRelSetExecutors{std::move(onCreateRelSetExecutors)},
          onMatchNodeSetExecutors{std::move(onMatchNodeSetExecutors)},
          onMatchRelSetExecutors{std::move(onMatchRelSetExecutors)}, info{std::move(info)} {}

    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    void executeOnMatch(ExecutionContext* context);

    void executeOnCreatedPattern(PatternCreationInfo& info, ExecutionContext* context);

    void executeOnNewPattern(PatternCreationInfo& info, ExecutionContext* context);

    void executeNoMatch(ExecutionContext* context);

private:
    std::vector<NodeInsertExecutor> nodeInsertExecutors;
    std::vector<RelInsertExecutor> relInsertExecutors;

    std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors;
    std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors;

    std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors;
    std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors;

    MergeInfo info;
    MergeLocalState localState;
};

} // namespace processor
} // namespace kuzu
