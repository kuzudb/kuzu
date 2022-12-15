#pragma once

#include <bitset>

#include "base_evaluator.h"
#include "common/exception.h"

namespace kuzu {
namespace evaluator {

struct CaseAlternativeEvaluator {
    unique_ptr<BaseExpressionEvaluator> whenEvaluator;
    unique_ptr<BaseExpressionEvaluator> thenEvaluator;
    unique_ptr<SelectionVector> whenSelVector;

    CaseAlternativeEvaluator(unique_ptr<BaseExpressionEvaluator> whenEvaluator,
        unique_ptr<BaseExpressionEvaluator> thenEvaluator)
        : whenEvaluator{std::move(whenEvaluator)}, thenEvaluator{std::move(thenEvaluator)} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager);

    inline unique_ptr<CaseAlternativeEvaluator> clone() {
        return make_unique<CaseAlternativeEvaluator>(
            whenEvaluator->clone(), thenEvaluator->clone());
    }
};

class CaseExpressionEvaluator : public BaseExpressionEvaluator {
public:
    CaseExpressionEvaluator(shared_ptr<Expression> expression,
        vector<unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators,
        unique_ptr<BaseExpressionEvaluator> elseEvaluator)
        : BaseExpressionEvaluator{}, expression{std::move(expression)},
          alternativeEvaluators{std::move(alternativeEvaluators)}, elseEvaluator{
                                                                       std::move(elseEvaluator)} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    void evaluate() override;

    bool select(SelectionVector& selVector) override;

    unique_ptr<BaseExpressionEvaluator> clone() override;

protected:
    void resolveResultVector(const ResultSet& resultSet, MemoryManager* memoryManager) override;

private:
    template<typename T>
    void fillEntry(sel_t resultPos, const ValueVector& thenVector);

    template<typename T>
    inline void fillSelected(const SelectionVector& selVector, const ValueVector& thenVector) {
        for (auto i = 0u; i < selVector.selectedSize; ++i) {
            auto resultPos = selVector.selectedPositions[i];
            fillEntry<T>(resultPos, thenVector);
        }
    }

    template<typename T>
    inline void fillAll(const ValueVector& thenVector) {
        auto resultSelVector = resultVector->state->selVector.get();
        for (auto i = 0u; i < resultSelVector->selectedSize; ++i) {
            auto resultPos = resultSelVector->selectedPositions[i];
            fillEntry<T>(resultPos, thenVector);
        }
    }

    void fillAllSwitch(const ValueVector& thenVector);

    void fillSelectedSwitch(const SelectionVector& selVector, const ValueVector& thenVector);

private:
    shared_ptr<Expression> expression;
    vector<unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators;
    unique_ptr<BaseExpressionEvaluator> elseEvaluator;

    bitset<DEFAULT_VECTOR_CAPACITY> filledMask;
};

} // namespace evaluator
} // namespace kuzu
