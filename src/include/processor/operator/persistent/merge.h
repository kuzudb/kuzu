#pragma once

#include "insert_executor.h"
#include "processor/operator/physical_operator.h"
#include "set_executor.h"

namespace kuzu {
namespace processor {

class Merge : public PhysicalOperator {
public:
    Merge(const DataPos& markPos,
        std::vector<std::unique_ptr<NodeInsertExecutor>> nodeInsertExecutors,
        std::vector<std::unique_ptr<RelInsertExecutor>> relInsertExecutors,
        std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors,
        std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors,
        std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors,
        std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::MERGE, std::move(child), id, paramsString},
          markPos{markPos}, nodeInsertExecutors{std::move(nodeInsertExecutors)},
          relInsertExecutors{std::move(relInsertExecutors)}, onCreateNodeSetExecutors{std::move(
                                                                 onCreateNodeSetExecutors)},
          onCreateRelSetExecutors{std::move(onCreateRelSetExecutors)},
          onMatchNodeSetExecutors{std::move(onMatchNodeSetExecutors)},
          onMatchRelSetExecutors{std::move(onMatchRelSetExecutors)} {}

    inline bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<Merge>(markPos, NodeInsertExecutor::copy(nodeInsertExecutors),
            RelInsertExecutor::copy(relInsertExecutors),
            NodeSetExecutor::copy(onCreateNodeSetExecutors),
            RelSetExecutor::copy(onCreateRelSetExecutors),
            NodeSetExecutor::copy(onMatchNodeSetExecutors),
            RelSetExecutor::copy(onMatchRelSetExecutors), children[0]->clone(), id, paramsString);
    }

private:
    DataPos markPos;
    common::ValueVector* markVector = nullptr;

    std::vector<std::unique_ptr<NodeInsertExecutor>> nodeInsertExecutors;
    std::vector<std::unique_ptr<RelInsertExecutor>> relInsertExecutors;

    std::vector<std::unique_ptr<NodeSetExecutor>> onCreateNodeSetExecutors;
    std::vector<std::unique_ptr<RelSetExecutor>> onCreateRelSetExecutors;

    std::vector<std::unique_ptr<NodeSetExecutor>> onMatchNodeSetExecutors;
    std::vector<std::unique_ptr<RelSetExecutor>> onMatchRelSetExecutors;
};

} // namespace processor
} // namespace kuzu
