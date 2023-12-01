#pragma once

#include <bitset>

#include "binder/expression/expression.h"
#include "expression_evaluator.h"

namespace kuzu {
namespace evaluator {

struct CaseAlternativeEvaluator {
    std::unique_ptr<ExpressionEvaluator> whenEvaluator;
    std::unique_ptr<ExpressionEvaluator> thenEvaluator;
    std::unique_ptr<common::SelectionVector> whenSelVector;

    CaseAlternativeEvaluator(std::unique_ptr<ExpressionEvaluator> whenEvaluator,
        std::unique_ptr<ExpressionEvaluator> thenEvaluator)
        : whenEvaluator{std::move(whenEvaluator)}, thenEvaluator{std::move(thenEvaluator)} {}

    void init(const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager);

    inline std::unique_ptr<CaseAlternativeEvaluator> clone() const {
        return make_unique<CaseAlternativeEvaluator>(
            whenEvaluator->clone(), thenEvaluator->clone());
    }
};

class CaseExpressionEvaluator : public ExpressionEvaluator {
public:
    CaseExpressionEvaluator(std::shared_ptr<binder::Expression> expression,
        std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators,
        std::unique_ptr<ExpressionEvaluator> elseEvaluator)
        : ExpressionEvaluator{}, expression{std::move(expression)},
          alternativeEvaluators{std::move(alternativeEvaluators)}, elseEvaluator{
                                                                       std::move(elseEvaluator)} {}

    void init(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) override;

    void evaluate() override;

    bool select(common::SelectionVector& selVector) override;

    std::unique_ptr<ExpressionEvaluator> clone() override;

protected:
    void resolveResultVector(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) override;

private:
    template<typename T>
    void fillEntry(common::sel_t resultPos, const common::ValueVector& thenVector);

    template<typename T>
    inline void fillSelected(
        const common::SelectionVector& selVector, const common::ValueVector& thenVector) {
        for (auto i = 0u; i < selVector.selectedSize; ++i) {
            auto resultPos = selVector.selectedPositions[i];
            fillEntry<T>(resultPos, thenVector);
        }
    }

    template<typename T>
    inline void fillAll(const common::ValueVector& thenVector) {
        auto resultSelVector = resultVector->state->selVector.get();
        for (auto i = 0u; i < resultSelVector->selectedSize; ++i) {
            auto resultPos = resultSelVector->selectedPositions[i];
            fillEntry<T>(resultPos, thenVector);
        }
    }

    void fillAllSwitch(const common::ValueVector& thenVector);

    void fillSelectedSwitch(
        const common::SelectionVector& selVector, const common::ValueVector& thenVector);

private:
    std::shared_ptr<binder::Expression> expression;
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators;
    std::unique_ptr<ExpressionEvaluator> elseEvaluator;

    std::bitset<common::DEFAULT_VECTOR_CAPACITY> filledMask;
};

} // namespace evaluator
} // namespace kuzu
