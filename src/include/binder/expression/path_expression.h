#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class PathExpression : public Expression {
public:
    PathExpression(common::LogicalType dataType, std::string uniqueName, std::string variableName,
        std::unique_ptr<common::LogicalType> nodeType, std::unique_ptr<common::LogicalType> relType,
        expression_vector children)
        : Expression{common::ExpressionType::PATH, std::move(dataType), std::move(children),
              std::move(uniqueName)},
          variableName{std::move(variableName)}, nodeType{std::move(nodeType)},
          relType{std::move(relType)} {}

    inline std::string getVariableName() const { return variableName; }
    inline common::LogicalType* getNodeType() const { return nodeType.get(); }
    inline common::LogicalType* getRelType() const { return relType.get(); }

    inline std::string toStringInternal() const final { return variableName; }

private:
    std::string variableName;
    std::unique_ptr<common::LogicalType> nodeType;
    std::unique_ptr<common::LogicalType> relType;
};

} // namespace binder
} // namespace kuzu
