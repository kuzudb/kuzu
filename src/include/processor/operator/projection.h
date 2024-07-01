#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Projection : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::PROJECTION;

public:
    Projection(std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> expressionEvaluators,
        std::vector<DataPos> expressionsOutputPos,
        std::unordered_set<uint32_t> discardedDataChunksPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator(type_, std::move(child), id, std::move(printInfo)),
          expressionEvaluators(std::move(expressionEvaluators)),
          expressionsOutputPos{std::move(expressionsOutputPos)},
          discardedDataChunksPos{std::move(discardedDataChunksPos)}, prevMultiplicity{1} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    void saveMultiplicity() { prevMultiplicity = resultSet->multiplicity; }

    void restoreMultiplicity() { resultSet->multiplicity = prevMultiplicity; }

private:
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> expressionEvaluators;
    std::vector<DataPos> expressionsOutputPos;
    std::unordered_set<uint32_t> discardedDataChunksPos;

    uint64_t prevMultiplicity;
};

} // namespace processor
} // namespace kuzu
