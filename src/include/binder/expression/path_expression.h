#pragma once

#include "node_expression.h"
#include "rel_expression.h"

namespace kuzu {
namespace binder {

class PathExpression : public Expression {
public:
    PathExpression(common::LogicalType dataType, std::string uniqueName, std::string variableName,
        std::shared_ptr<NodeExpression> node, std::shared_ptr<RelExpression> rel,
        expression_vector children)
        : Expression{common::PATH, std::move(dataType), std::move(children), std::move(uniqueName)},
          variableName{std::move(variableName)}, node{std::move(node)}, rel{std::move(rel)} {}

    inline std::string getVariableName() const { return variableName; }
    inline std::shared_ptr<NodeExpression> getNode() const { return node; }
    inline std::shared_ptr<RelExpression> getRel() const { return rel; }

    inline std::string toString() const override { return variableName; }

private:
    std::string variableName;
    std::shared_ptr<NodeExpression> node;
    std::shared_ptr<RelExpression> rel;
};

} // namespace binder
} // namespace kuzu
