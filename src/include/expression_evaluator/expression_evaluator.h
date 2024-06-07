#pragma once

#include "main/client_context.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace evaluator {

struct EvaluatorLocalState {
    main::ClientContext* clientContext;
    uint64_t count;
};

class ExpressionEvaluator {
public:
    ExpressionEvaluator() = default;
    // Leaf evaluators (reference or literal)
    explicit ExpressionEvaluator(bool isResultFlat) : isResultFlat_{isResultFlat} {}
    explicit ExpressionEvaluator(std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : children{std::move(children)} {}
    virtual ~ExpressionEvaluator() = default;

    inline bool isResultFlat() const { return isResultFlat_; }

    virtual void init(const processor::ResultSet& resultSet, main::ClientContext* clientContext);

    virtual void evaluate() = 0;

    virtual bool select(common::SelectionVector& selVector) = 0;

    virtual std::unique_ptr<ExpressionEvaluator> clone() = 0;

    EvaluatorLocalState& getLocalStateRef() { return localState; }

    static std::vector<std::unique_ptr<ExpressionEvaluator>> copy(
        const std::vector<std::unique_ptr<ExpressionEvaluator>>& evaluators);

protected:
    virtual void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) = 0;

    void resolveResultStateFromChildren(const std::vector<ExpressionEvaluator*>& inputEvaluators);

public:
    std::shared_ptr<common::ValueVector> resultVector;

protected:
    bool isResultFlat_ = true;
    std::vector<std::unique_ptr<ExpressionEvaluator>> children;
    EvaluatorLocalState localState;
};

} // namespace evaluator
} // namespace kuzu
