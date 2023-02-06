#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/graph_pattern/pattern_element.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

class MatchClause : public ReadingClause {
public:
    explicit MatchClause(
        std::vector<std::unique_ptr<PatternElement>> patternElements, bool isOptional = false)
        : ReadingClause{common::ClauseType::MATCH}, patternElements{std::move(patternElements)},
          isOptional{isOptional} {}
    ~MatchClause() override = default;

    inline const std::vector<std::unique_ptr<PatternElement>>& getPatternElements() const {
        return patternElements;
    }

    inline void setWhereClause(std::unique_ptr<ParsedExpression> expression) {
        whereClause = std::move(expression);
    }

    inline bool hasWhereClause() const { return whereClause != nullptr; }
    inline ParsedExpression* getWhereClause() const { return whereClause.get(); }

    inline bool getIsOptional() const { return isOptional; }

private:
    std::vector<std::unique_ptr<PatternElement>> patternElements;
    std::unique_ptr<ParsedExpression> whereClause;
    bool isOptional;
};

} // namespace parser
} // namespace kuzu
