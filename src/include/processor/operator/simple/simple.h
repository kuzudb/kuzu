#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Simple : public PhysicalOperator {
public:
    Simple(PhysicalOperatorType operatorType, const DataPos& outputPos, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{operatorType, id, std::move(printInfo)}, outputPos{outputPos},
          outputVector{nullptr}, hasExecuted{false} {}

    bool isSource() const final { return true; }
    bool isParallel() const final { return false; }

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
