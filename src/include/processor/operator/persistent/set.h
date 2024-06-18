#pragma once

#include "processor/operator/physical_operator.h"
#include "set_executor.h"

namespace kuzu {
namespace processor {

class SetNodeProperty : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SET_PROPERTY;

public:
    SetNodeProperty(std::vector<std::unique_ptr<NodeSetExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          executors{std::move(executors)} {}

    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<NodeSetExecutor>> executors;
};

class SetRelProperty : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SET_PROPERTY;

public:
    SetRelProperty(std::vector<std::unique_ptr<RelSetExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          executors{std::move(executors)} {}

    inline bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<RelSetExecutor>> executors;
};

} // namespace processor
} // namespace kuzu
