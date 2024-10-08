#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct ProjectionPrintInfo final : OPPrintInfo {
    binder::expression_vector expressions;

    explicit ProjectionPrintInfo(binder::expression_vector expressions)
        : expressions{std::move(expressions)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<ProjectionPrintInfo>(new ProjectionPrintInfo(*this));
    }

private:
    ProjectionPrintInfo(const ProjectionPrintInfo& other)
        : OPPrintInfo{other}, expressions{other.expressions} {}
};

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
          discardedDataChunksPos{std::move(discardedDataChunksPos)}, prevMultiplicity{1} {
        for (auto i = 0u; i < this->expressionsOutputPos.size(); ++i) {
            expressionsOutputDataChunksPos.insert(this->expressionsOutputPos[i].dataChunkPos);
        }
    }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    void saveMultiplicity() { prevMultiplicity = resultSet->multiplicity; }

    void restoreMultiplicity() { resultSet->multiplicity = prevMultiplicity; }

private:
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> expressionEvaluators;
    std::vector<DataPos> expressionsOutputPos;
    std::unordered_set<uint32_t> expressionsOutputDataChunksPos;
    std::unordered_set<uint32_t> discardedDataChunksPos;

    uint64_t prevMultiplicity;
};

} // namespace processor
} // namespace kuzu
