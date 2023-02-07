#pragma once

#include <functional>

#include "binder/expression/expression.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace evaluator {

class BaseExpressionEvaluator {
public:
    BaseExpressionEvaluator() = default;
    // Leaf evaluators (reference or literal)
    explicit BaseExpressionEvaluator(bool isResultFlat) : isResultFlat_{isResultFlat} {}

    explicit BaseExpressionEvaluator(std::vector<std::unique_ptr<BaseExpressionEvaluator>> children)
        : children{std::move(children)} {}

    virtual ~BaseExpressionEvaluator() = default;

    inline bool isResultFlat() const { return isResultFlat_; }

    virtual void init(const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager);

    virtual void evaluate() = 0;

    virtual bool select(common::SelectionVector& selVector) = 0;

    virtual std::unique_ptr<BaseExpressionEvaluator> clone() = 0;

protected:
    virtual void resolveResultVector(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) = 0;

    void resolveResultStateFromChildren(
        const std::vector<BaseExpressionEvaluator*>& inputEvaluators);

public:
    std::shared_ptr<common::ValueVector> resultVector;

protected:
    bool isResultFlat_ = true;
    std::vector<std::unique_ptr<BaseExpressionEvaluator>> children;
};

} // namespace evaluator
} // namespace kuzu
