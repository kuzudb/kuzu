#pragma once

#include "main/client_context.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace evaluator {

struct EvaluatorLocalState {
    main::ClientContext* clientContext;
    uint64_t count;
};

enum class EvaluatorType : uint8_t {
    CASE_ELSE = 0,
    FUNCTION = 1,
    LAMBDA_PARAM = 2,
    LIST_LAMBDA = 3,
    LITERAL = 4,
    PATH = 5,
    NODE_REL = 6,
    REFERENCE = 8,
};

class ExpressionEvaluator;
using evaluator_vector_t = std::vector<std::unique_ptr<ExpressionEvaluator>>;

class ExpressionEvaluator {
public:
    explicit ExpressionEvaluator(EvaluatorType type, std::shared_ptr<binder::Expression> expression)
        : type{type}, expression{std::move(expression)} {};
    ExpressionEvaluator(EvaluatorType type, std::shared_ptr<binder::Expression> expression,
        bool isResultFlat)
        : type{type}, expression{std::move(expression)}, isResultFlat_{isResultFlat} {}
    ExpressionEvaluator(EvaluatorType type, std::shared_ptr<binder::Expression> expression,
        evaluator_vector_t children)
        : type{type}, expression{std::move(expression)}, children{std::move(children)} {}
    ExpressionEvaluator(const ExpressionEvaluator& other)
        : type{other.type}, expression{other.expression}, isResultFlat_{other.isResultFlat_},
          children{cloneVector(other.children)} {}
    virtual ~ExpressionEvaluator() = default;

    EvaluatorType getEvaluatorType() const { return type; }

    const common::LogicalType& getResultDataType() const { return expression->getDataType(); }
    void setResultFlat(bool val) { isResultFlat_ = val; }
    bool isResultFlat() const { return isResultFlat_; }

    const evaluator_vector_t& getChildren() const { return children; }

    virtual void init(const processor::ResultSet& resultSet, main::ClientContext* clientContext);

    virtual void evaluate() = 0;

    virtual bool select(common::SelectionVector& selVector) = 0;

    virtual std::unique_ptr<ExpressionEvaluator> clone() = 0;

    EvaluatorLocalState& getLocalStateUnsafe() { return localState; }

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ExpressionEvaluator&, const TARGET&>(*this);
    }
    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<ExpressionEvaluator&, TARGET&>(*this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<ExpressionEvaluator*, TARGET*>(this);
    }

protected:
    virtual void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) = 0;

    void resolveResultStateFromChildren(const std::vector<ExpressionEvaluator*>& inputEvaluators);

public:
    std::shared_ptr<common::ValueVector> resultVector;

protected:
    EvaluatorType type;
    std::shared_ptr<binder::Expression> expression;
    bool isResultFlat_ = true;
    evaluator_vector_t children;
    EvaluatorLocalState localState;
};

} // namespace evaluator
} // namespace kuzu
