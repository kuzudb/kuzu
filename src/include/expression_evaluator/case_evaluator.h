#pragma once

#include <bitset>

#include "binder/expression/expression.h"
#include "expression_evaluator.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace evaluator {

struct CaseAlternativeEvaluator {
    std::unique_ptr<ExpressionEvaluator> whenEvaluator;
    std::unique_ptr<ExpressionEvaluator> thenEvaluator;
    std::unique_ptr<common::SelectionVector> whenSelVector;

    CaseAlternativeEvaluator(std::unique_ptr<ExpressionEvaluator> whenEvaluator,
        std::unique_ptr<ExpressionEvaluator> thenEvaluator)
        : whenEvaluator{std::move(whenEvaluator)}, thenEvaluator{std::move(thenEvaluator)} {}

    void init(const processor::ResultSet& resultSet, main::ClientContext* clientContext);

    std::unique_ptr<CaseAlternativeEvaluator> copy() const {
        return make_unique<CaseAlternativeEvaluator>(whenEvaluator->clone(),
            thenEvaluator->clone());
    }
};

class CaseExpressionEvaluator : public ExpressionEvaluator {
    static constexpr EvaluatorType type_ = EvaluatorType::CASE_ELSE;

public:
    CaseExpressionEvaluator(std::shared_ptr<binder::Expression> expression,
        std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators,
        std::unique_ptr<ExpressionEvaluator> elseEvaluator)
        : ExpressionEvaluator{type_, std::move(expression)},
          alternativeEvaluators{std::move(alternativeEvaluators)},
          elseEvaluator{std::move(elseEvaluator)} {}

    const std::vector<std::unique_ptr<CaseAlternativeEvaluator>>& getAlternativeEvaluators() const {
        return alternativeEvaluators;
    }
    ExpressionEvaluator* getElseEvaluator() const { return elseEvaluator.get(); }

    void init(const processor::ResultSet& resultSet, main::ClientContext* clientContext) override;

    void evaluate() override;

    bool select(common::SelectionVector& selVector) override;

    std::unique_ptr<ExpressionEvaluator> clone() override {
        return std::make_unique<CaseExpressionEvaluator>(expression,
            copyVector(alternativeEvaluators), elseEvaluator->clone());
    }

protected:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

private:
    void fillSelected(const common::SelectionVector& selVector, common::ValueVector* srcVector);

    void fillAll(common::ValueVector* srcVector);

    void fillEntry(common::sel_t resultPos, common::ValueVector* srcVector);

private:
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators;
    std::unique_ptr<ExpressionEvaluator> elseEvaluator;

    std::bitset<common::DEFAULT_VECTOR_CAPACITY> filledMask;
};

} // namespace evaluator
} // namespace kuzu
