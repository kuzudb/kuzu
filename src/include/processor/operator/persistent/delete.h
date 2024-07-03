#pragma once

#include "delete_executor.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class DeleteNode : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::DELETE_;

public:
    DeleteNode(std::vector<std::unique_ptr<NodeDeleteExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          executors{std::move(executors)} {}

    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<NodeDeleteExecutor>> executors;
};

class DeleteRel : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::DELETE_;

public:
    DeleteRel(std::vector<std::unique_ptr<RelDeleteExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          executors{std::move(executors)} {}

    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<RelDeleteExecutor>> executors;
};

struct DeleteNodePrintInfo final : OPPrintInfo {
    binder::expression_vector expressions;
    std::string deleteConfig;

    DeleteNodePrintInfo(binder::expression_vector expressions, std::string deleteConfig)
        : expressions{std::move(expressions)}, deleteConfig{std::move(deleteConfig)} {}
    DeleteNodePrintInfo(const DeleteNodePrintInfo& other)
        : OPPrintInfo{other}, expressions{other.expressions}, deleteConfig{other.deleteConfig} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<DeleteNodePrintInfo>(*this);
    }
};

} // namespace processor
} // namespace kuzu
