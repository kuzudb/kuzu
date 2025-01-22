#pragma once

#include "binder/bound_statement.h"
#include "function/scalar_macro_function.h"

namespace kuzu {
namespace binder {

class BoundCreateMacro final : public BoundStatement {
public:
    explicit BoundCreateMacro(std::string macroName,
        std::unique_ptr<function::ScalarMacroFunction> macro)
        : BoundStatement{common::StatementType::CREATE_MACRO,
              BoundStatementResult::createSingleStringColumnResult("result" /* columnName */)},
          macroName{std::move(macroName)}, macro{std::move(macro)} {}

    inline std::string getMacroName() const { return macroName; }

    inline std::unique_ptr<function::ScalarMacroFunction> getMacro() const { return macro->copy(); }

private:
    std::string macroName;
    std::unique_ptr<function::ScalarMacroFunction> macro;
};

} // namespace binder
} // namespace kuzu
