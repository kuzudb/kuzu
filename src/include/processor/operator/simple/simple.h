#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Simple : public PhysicalOperator {
public:
    Simple(PhysicalOperatorType operatorType, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, outputPos{outputPos},
          outputVector{nullptr}, hasExecuted{false} {}
    Simple(PhysicalOperatorType operatorType,
        std::vector<std::unique_ptr<PhysicalOperator>> children, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, std::move(children), id, paramsString},
          outputPos{outputPos}, outputVector{nullptr}, hasExecuted{false} {}

    bool isSource() const final { return true; }
    bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

protected:
    virtual std::string getOutputMsg() = 0;
    virtual void executeInternal(ExecutionContext* context) = 0;

protected:
    DataPos outputPos;
    common::ValueVector* outputVector;

    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
