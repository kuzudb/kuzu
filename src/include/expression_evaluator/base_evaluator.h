#pragma once

#include <functional>

#include "binder/expression/expression.h"
#include "processor/result/result_set.h"

using namespace kuzu::binder;
using namespace kuzu::processor;

namespace kuzu {
namespace evaluator {

using binary_exec_operation = std::function<void(ValueVector&, ValueVector&, ValueVector&)>;
using binary_select_operation = std::function<uint64_t(ValueVector&, ValueVector&, sel_t*)>;
using unary_exec_operation = std::function<void(ValueVector&, ValueVector&)>;
using unary_select_operation = std::function<uint64_t(ValueVector&, sel_t*)>;

class BaseExpressionEvaluator {

public:
    // Literal or reference evaluator
    BaseExpressionEvaluator() = default;

    // Unary evaluator
    explicit BaseExpressionEvaluator(unique_ptr<BaseExpressionEvaluator> child);

    // Binary evaluator
    BaseExpressionEvaluator(
        unique_ptr<BaseExpressionEvaluator> left, unique_ptr<BaseExpressionEvaluator> right);

    // Function evaluator
    explicit BaseExpressionEvaluator(vector<unique_ptr<BaseExpressionEvaluator>> children)
        : children{move(children)} {}

    virtual ~BaseExpressionEvaluator() = default;

    virtual void init(const ResultSet& resultSet, MemoryManager* memoryManager);

    virtual void evaluate() = 0;

    virtual bool select(SelectionVector& selVector) = 0;

    virtual unique_ptr<BaseExpressionEvaluator> clone() = 0;

public:
    shared_ptr<ValueVector> resultVector;

protected:
    vector<unique_ptr<BaseExpressionEvaluator>> children;
};

} // namespace evaluator
} // namespace kuzu
