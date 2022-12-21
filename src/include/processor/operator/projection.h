#pragma once

#include <cassert>

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class Projection : public PhysicalOperator {
public:
    Projection(vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators,
        vector<DataPos> expressionsOutputPos, unordered_set<uint32_t> discardedDataChunksPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator(PhysicalOperatorType::PROJECTION, std::move(child), id, paramsString),
          expressionEvaluators(std::move(expressionEvaluators)), expressionsOutputPos{std::move(
                                                                     expressionsOutputPos)},
          discardedDataChunksPos{std::move(discardedDataChunksPos)}, prevMultiplicity{1} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    inline void saveMultiplicity() { prevMultiplicity = resultSet->multiplicity; }

    inline void restoreMultiplicity() { resultSet->multiplicity = prevMultiplicity; }

private:
    vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators;
    vector<DataPos> expressionsOutputPos;
    unordered_set<uint32_t> discardedDataChunksPos;

    uint64_t prevMultiplicity;
};

} // namespace processor
} // namespace kuzu
