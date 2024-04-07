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

    void init(const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager);

    inline std::unique_ptr<CaseAlternativeEvaluator> clone() const {
        return make_unique<CaseAlternativeEvaluator>(whenEvaluator->clone(),
            thenEvaluator->clone());
    }
};

class CaseExpressionEvaluator : public ExpressionEvaluator {
public:
    CaseExpressionEvaluator(std::shared_ptr<binder::Expression> expression,
        std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators,
        std::unique_ptr<ExpressionEvaluator> elseEvaluator)
        : ExpressionEvaluator{}, expression{std::move(expression)},
          alternativeEvaluators{std::move(alternativeEvaluators)},
          elseEvaluator{std::move(elseEvaluator)} {}

    void init(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

    void evaluate(main::ClientContext* clientContext) override;

    bool select(common::SelectionVector& selVector, main::ClientContext* clientContext) override;

    std::unique_ptr<ExpressionEvaluator> clone() override;

protected:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

private:
    void fillSelected(const common::SelectionVector& selVector, common::ValueVector* srcVector);

    void fillAll(common::ValueVector* srcVector);

    void fillEntry(common::sel_t resultPos, common::ValueVector* srcVector);

private:
    std::shared_ptr<binder::Expression> expression;
    std::vector<std::unique_ptr<CaseAlternativeEvaluator>> alternativeEvaluators;
    std::unique_ptr<ExpressionEvaluator> elseEvaluator;

    std::bitset<common::DEFAULT_VECTOR_CAPACITY> filledMask;
};

} // namespace evaluator
} // namespace kuzu
