#pragma once

#include <functional>

#include "binder/expression/expression.h"
#include "processor/result/result_set.h"

using namespace kuzu::binder;
using namespace kuzu::processor;

namespace kuzu {
namespace evaluator {

class BaseExpressionEvaluator {
public:
    BaseExpressionEvaluator() = default;
    // Leaf evaluators (reference or literal)
    explicit BaseExpressionEvaluator(bool isResultFlat) : isResultFlat_{isResultFlat} {}

    explicit BaseExpressionEvaluator(vector<unique_ptr<BaseExpressionEvaluator>> children)
        : children{std::move(children)} {}

    virtual ~BaseExpressionEvaluator() = default;

    inline bool isResultFlat() const { return isResultFlat_; }

    virtual void init(const ResultSet& resultSet, MemoryManager* memoryManager);

    virtual void evaluate() = 0;

    virtual bool select(SelectionVector& selVector) = 0;

    virtual unique_ptr<BaseExpressionEvaluator> clone() = 0;

protected:
    virtual void resolveResultVector(const ResultSet& resultSet, MemoryManager* memoryManager) = 0;

    void resolveResultStateFromChildren(const vector<BaseExpressionEvaluator*>& inputEvaluators);

public:
    shared_ptr<ValueVector> resultVector;

protected:
    bool isResultFlat_ = true;
    vector<unique_ptr<BaseExpressionEvaluator>> children;
};

} // namespace evaluator
} // namespace kuzu
