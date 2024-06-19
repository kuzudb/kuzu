#pragma once

#include "insert_executor.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Insert : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::INSERT;

public:
    Insert(std::vector<NodeInsertExecutor> nodeExecutors,
        std::vector<RelInsertExecutor> relExecutors, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          nodeExecutors{std::move(nodeExecutors)}, relExecutors{std::move(relExecutors)} {}

    inline bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<Insert>(copyVector(nodeExecutors), copyVector(relExecutors),
            children[0]->clone(), id, printInfo->copy());
    }

private:
    std::vector<NodeInsertExecutor> nodeExecutors;
    std::vector<RelInsertExecutor> relExecutors;
};
} // namespace processor
} // namespace kuzu
