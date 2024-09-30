#pragma once

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class VoidFunctionCall : public Statement {
public:
    explicit VoidFunctionCall(std::string functionName)
        : Statement{common::StatementType::VOID_FUNCTION_CALL},
          functionName{std::move(functionName)} {}

    std::string getFunctionName() const { return functionName; }

private:
    std::string functionName;
};

} // namespace parser
} // namespace kuzu
