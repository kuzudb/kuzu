#pragma once

#include "base_evaluator.h"

namespace graphflow {
namespace evaluator {

class OperatorExpressionEvaluator : public BaseExpressionEvaluator {

public:
    OperatorExpressionEvaluator(
        shared_ptr<Expression> expression, unique_ptr<BaseExpressionEvaluator> child)
        : BaseExpressionEvaluator{move(child)}, expression{move(expression)} {}

    OperatorExpressionEvaluator(shared_ptr<Expression> expression,
        unique_ptr<BaseExpressionEvaluator> left, unique_ptr<BaseExpressionEvaluator> right)
        : BaseExpressionEvaluator{move(left), move(right)}, expression{move(expression)} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    virtual void getExecOperation() = 0;

    void evaluate() override = 0;

    uint64_t select(sel_t* selectedPos) override = 0;

    unique_ptr<BaseExpressionEvaluator> clone() override = 0;

protected:
    shared_ptr<Expression> expression;
};

class UnaryOperatorExpressionEvaluator : public OperatorExpressionEvaluator {

public:
    UnaryOperatorExpressionEvaluator(
        shared_ptr<Expression> expression, unique_ptr<BaseExpressionEvaluator> child)
        : OperatorExpressionEvaluator{move(expression), move(child)} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    void evaluate() override;

    uint64_t select(sel_t* selectedPos) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<UnaryOperatorExpressionEvaluator>(expression, children[0]->clone());
    }

private:
    void getExecOperation() override;

private:
    unary_exec_operation execOperation;
    unary_select_operation selectOperation;
};

class BinaryOperatorExpressionEvaluator : public OperatorExpressionEvaluator {

public:
    BinaryOperatorExpressionEvaluator(shared_ptr<Expression> expression,
        unique_ptr<BaseExpressionEvaluator> left, unique_ptr<BaseExpressionEvaluator> right)
        : OperatorExpressionEvaluator{move(expression), move(left), move(right)} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    void evaluate() override;

    uint64_t select(sel_t* selectedPos) override;

    unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<BinaryOperatorExpressionEvaluator>(
            expression, children[0]->clone(), children[1]->clone());
    }

private:
    void getExecOperation() override;

private:
    binary_exec_operation execOperation;
    binary_select_operation selectOperation;
};

} // namespace evaluator
} // namespace graphflow
