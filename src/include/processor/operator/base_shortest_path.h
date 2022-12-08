#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class BaseShortestPath : public PhysicalOperator {
public:
    BaseShortestPath(const DataPos& srcDataPos, const DataPos& destDataPos, uint64_t lowerBound,
        uint64_t upperBound, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, srcDataPos{srcDataPos},
          destDataPos{destDataPos}, lowerBound{lowerBound}, upperBound{upperBound} {}

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::SHORTEST_PATH;
    }

    virtual bool getNextTuplesInternal() = 0;

    shared_ptr<ResultSet> init(ExecutionContext* context);

    virtual unique_ptr<PhysicalOperator> clone() = 0;

protected:
    DataPos srcDataPos;
    DataPos destDataPos;
    uint64_t lowerBound;
    uint64_t upperBound;
    shared_ptr<ValueVector> srcValueVector;
    shared_ptr<ValueVector> destValueVector;
};

} // namespace processor
} // namespace kuzu
