#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Call : public Statement {
public:
    explicit Call(common::StatementType statementType, std::string optionName,
        std::unique_ptr<ParsedExpression> optionValue)
        : Statement{statementType}, optionName{std::move(optionName)}, optionValue{std::move(
                                                                           optionValue)} {}

    inline std::string getOptionName() const { return optionName; }

    inline ParsedExpression* getOptionValue() const { return optionValue.get(); }

private:
    std::string optionName;
    std::unique_ptr<ParsedExpression> optionValue;
};

} // namespace parser
} // namespace kuzu
