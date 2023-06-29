#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/reading_clause/reading_clause.h"

namespace kuzu {
namespace parser {

class InQueryCall : public ReadingClause {
public:
    InQueryCall(std::string optionName, std::unique_ptr<ParsedExpression> optionValue)
        : ReadingClause{common::ClauseType::InQueryCall}, funcName{std::move(optionName)},
          parameter{std::move(optionValue)} {}

    inline std::string getFuncName() const { return funcName; }

    inline ParsedExpression* getParameter() const { return parameter.get(); }

private:
    std::string funcName;
    std::unique_ptr<ParsedExpression> parameter;
};

} // namespace parser
} // namespace kuzu
