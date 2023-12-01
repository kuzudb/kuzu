#pragma once

#include "insert_executor.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class InsertNode : public PhysicalOperator {
public:
    InsertNode(std::vector<std::unique_ptr<NodeInsertExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INSERT_NODE, std::move(child), id, paramsString},
          executors{std::move(executors)} {}

    inline bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<InsertNode>(
            NodeInsertExecutor::copy(executors), children[0]->clone(), id, paramsString);
    }

private:
    std::vector<std::unique_ptr<NodeInsertExecutor>> executors;
};

class InsertRel : public PhysicalOperator {
public:
    InsertRel(std::vector<std::unique_ptr<RelInsertExecutor>> executors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INSERT_REL, std::move(child), id, paramsString},
          executors{std::move(executors)} {}

    inline bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<InsertRel>(
            RelInsertExecutor::copy(executors), children[0]->clone(), id, paramsString);
    }

private:
    std::vector<std::unique_ptr<RelInsertExecutor>> executors;
};

} // namespace processor
} // namespace kuzu
