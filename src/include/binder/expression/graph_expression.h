#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

// Represent on disk graph
class GraphExpression : public Expression {
    static constexpr common::ExpressionType exprType = common::ExpressionType::GRAPH;

public:
    GraphExpression(std::string uniqueName, std::string nodeName, std::string relName)
        : Expression{exprType, *common::LogicalType::ANY(), std::move(uniqueName)},
          nodeName{std::move(nodeName)}, relName{std::move(relName)} {}

    std::string getNodeName() const { return nodeName; }
    std::string getRelName() const { return relName; }

protected:
    std::string toStringInternal() const override { return alias.empty() ? uniqueName : alias; }

private:
    std::string nodeName;
    std::string relName;
};

} // namespace binder
} // namespace kuzu
