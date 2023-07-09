#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/reading_clause/reading_clause.h"

namespace kuzu {
namespace parser {

class InQueryCallClause : public ReadingClause {
public:
    InQueryCallClause(
        std::string optionName, std::vector<std::unique_ptr<ParsedExpression>> parameters)
        : ReadingClause{common::ClauseType::InQueryCall}, funcName{std::move(optionName)},
          parameters{std::move(parameters)} {}

    inline std::string getFuncName() const { return funcName; }

    std::vector<ParsedExpression*> getParameters() const;

private:
    std::string funcName;
    std::vector<std::unique_ptr<ParsedExpression>> parameters;
};

} // namespace parser
} // namespace kuzu
