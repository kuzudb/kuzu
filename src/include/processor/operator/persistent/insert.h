#pragma once

#include "insert_executor.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Insert : public PhysicalOperator {
public:
    Insert(std::vector<NodeInsertExecutor> nodeExecutors,
        std::vector<RelInsertExecutor> relExecutors, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INSERT, std::move(child), id, paramsString},
          nodeExecutors{std::move(nodeExecutors)}, relExecutors{std::move(relExecutors)} {}

    inline bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<Insert>(copyVector(nodeExecutors), copyVector(relExecutors),
            children[0]->clone(), id, paramsString);
    }

private:
    std::vector<NodeInsertExecutor> nodeExecutors;
    std::vector<RelInsertExecutor> relExecutors;
};
} // namespace processor
} // namespace kuzu
