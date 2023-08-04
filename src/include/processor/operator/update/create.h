#pragma once

#include "insert_executor.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class CreateNode : public PhysicalOperator {
public:
    CreateNode(std::vector<std::unique_ptr<NodeInsertExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CREATE_NODE, std::move(child), id, paramsString},
          executors{std::move(executors)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<NodeInsertExecutor>> executors;
};

class CreateRel : public PhysicalOperator {
public:
    CreateRel(std::vector<std::unique_ptr<RelInsertExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CREATE_REL, std::move(child), id, paramsString},
          executors{std::move(executors)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<RelInsertExecutor>> executors;
};

} // namespace processor
} // namespace kuzu
