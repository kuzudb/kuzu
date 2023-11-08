#pragma once

#include "common/assert.h"
#include "parsed_expression.h"
#include "parser/query/graph_pattern/pattern_element.h"

namespace kuzu {
namespace parser {

class ParsedSubqueryExpression : public ParsedExpression {
public:
    ParsedSubqueryExpression(
        std::vector<std::unique_ptr<PatternElement>> patternElements, std::string rawName)
        : ParsedExpression{common::ExpressionType::EXISTENTIAL_SUBQUERY, std::move(rawName)},
          patternElements{std::move(patternElements)} {}

    ParsedSubqueryExpression(common::ExpressionType type, std::string alias, std::string rawName,
        parsed_expression_vector children,
        std::vector<std::unique_ptr<PatternElement>> patternElements,
        std::unique_ptr<ParsedExpression> whereClause)
        : ParsedExpression{type, std::move(alias), std::move(rawName), std::move(children)},
          patternElements{std::move(patternElements)}, whereClause{std::move(whereClause)} {}

    inline const std::vector<std::unique_ptr<PatternElement>>& getPatternElements() const {
        return patternElements;
    }

    inline void setWhereClause(std::unique_ptr<ParsedExpression> expression) {
        whereClause = std::move(expression);
    }
    inline bool hasWhereClause() const { return whereClause != nullptr; }
    inline ParsedExpression* getWhereClause() const { return whereClause.get(); }

    static std::unique_ptr<ParsedSubqueryExpression> deserialize(
        common::Deserializer& /*deserializer*/) {
        KU_UNREACHABLE;
    }

    std::unique_ptr<ParsedExpression> copy() const override { KU_UNREACHABLE; }

private:
    void serializeInternal(common::Serializer& /*serializer*/) const override { KU_UNREACHABLE; }

private:
    std::vector<std::unique_ptr<PatternElement>> patternElements;
    std::unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace kuzu
