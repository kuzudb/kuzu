#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {

enum class SemiMaskType : uint8_t {
    NODE = 0,
    PATH = 1,
};

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(SemiMaskType type, std::shared_ptr<binder::Expression> key,
        std::shared_ptr<binder::NodeExpression> node, std::vector<LogicalOperator*> ops,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SEMI_MASKER, std::move(child)}, type{type},
          key{std::move(key)}, node{std::move(node)}, ops{std::move(ops)} {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override { return node->toString(); }

    inline SemiMaskType getType() const { return type; }
    inline std::shared_ptr<binder::Expression> getKey() const { return key; }
    inline std::shared_ptr<binder::NodeExpression> getNode() const { return node; }
    inline std::vector<LogicalOperator*> getOperators() const { return ops; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        throw common::RuntimeException("LogicalSemiMasker::copy() should not be called.");
    }

private:
    SemiMaskType type;
    std::shared_ptr<binder::Expression> key;
    std::shared_ptr<binder::NodeExpression> node;
    std::vector<LogicalOperator*> ops;
};

} // namespace planner
} // namespace kuzu
