#pragma once

#include "processor/operator/physical_operator.h"
#include "set_executor.h"

namespace kuzu {
namespace processor {

class SetNodeProperty : public PhysicalOperator {
public:
    SetNodeProperty(std::vector<std::unique_ptr<NodeSetExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SET_NODE_PROPERTY, std::move(child), id,
              paramsString},
          executors{std::move(executors)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<NodeSetExecutor>> executors;
};

class SetRelProperty : public PhysicalOperator {
public:
    SetRelProperty(std::vector<std::unique_ptr<RelSetExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SET_NODE_PROPERTY, std::move(child), id,
              paramsString},
          executors{std::move(executors)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<RelSetExecutor>> executors;
};

} // namespace processor
} // namespace kuzu
