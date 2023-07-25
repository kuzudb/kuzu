#pragma once

#include "common/exception.h"
#include "parsed_expression.h"
#include "parser/query/reading_clause/match_clause.h"

namespace kuzu {
namespace parser {

class ParsedSubqueryExpression : public ParsedExpression {
public:
    ParsedSubqueryExpression(
        std::vector<std::unique_ptr<PatternElement>> patternElements, std::string rawName)
        : ParsedExpression{common::EXISTENTIAL_SUBQUERY, std::move(rawName)},
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
        common::FileInfo* fileInfo, uint64_t& offset) {
        throw common::NotImplementedException{"ParsedSubqueryExpression::deserialize()"};
    }

    std::unique_ptr<ParsedExpression> copy() const override {
        throw common::NotImplementedException{"ParsedPropertyExpression::copy()"};
    }

private:
    void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) const override {
        throw common::NotImplementedException{"ParsedSubqueryExpression::serializeInternal()"};
    }

private:
    std::vector<std::unique_ptr<PatternElement>> patternElements;
    std::unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace kuzu
