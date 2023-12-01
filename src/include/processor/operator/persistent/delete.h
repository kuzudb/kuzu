#pragma once

#include "delete_executor.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class DeleteNode : public PhysicalOperator {
public:
    DeleteNode(std::vector<std::unique_ptr<NodeDeleteExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::DELETE_NODE, std::move(child), id, paramsString},
          executors{std::move(executors)} {}

    inline bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<NodeDeleteExecutor>> executors;
};

class DeleteRel : public PhysicalOperator {
public:
    DeleteRel(std::vector<std::unique_ptr<RelDeleteExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::DELETE_REL, std::move(child), id, paramsString},
          executors{std::move(executors)} {}

    inline bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    std::vector<std::unique_ptr<RelDeleteExecutor>> executors;
};

} // namespace processor
} // namespace kuzu
