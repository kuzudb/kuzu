#pragma once

#include "insert_executor.h"
#include "processor/operator/physical_operator.h"
#include "set_executor.h"

namespace kuzu {
namespace processor {

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

class Merge : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::MERGE;

public:
    Merge(const DataPos& existenceMark, const DataPos& distinctMark,
        std::vector<NodeInsertExecutor> nodeInsertExecutors,
        std::vector<RelInsertExecutor> relInsertExecutors,
        std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors,
        std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors,
        std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors,
        std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          existenceMark{existenceMark}, distinctMark{distinctMark},
          nodeInsertExecutors{std::move(nodeInsertExecutors)},
          relInsertExecutors{std::move(relInsertExecutors)},
          onCreateNodeSetExecutors{std::move(onCreateNodeSetExecutors)},
          onCreateRelSetExecutors{std::move(onCreateRelSetExecutors)},
          onMatchNodeSetExecutors{std::move(onMatchNodeSetExecutors)},
          onMatchRelSetExecutors{std::move(onMatchRelSetExecutors)} {}

    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    DataPos existenceMark;
    DataPos distinctMark;
    common::ValueVector* existenceVector = nullptr;
    common::ValueVector* distinctVector = nullptr;

    std::vector<NodeInsertExecutor> nodeInsertExecutors;
    std::vector<RelInsertExecutor> relInsertExecutors;

    std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors;
    std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors;

    std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors;
    std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors;
};

} // namespace processor
} // namespace kuzu
