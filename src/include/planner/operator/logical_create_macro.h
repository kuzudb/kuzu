#pragma once

#include "function/scalar_macro_function.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCreateMacro : public LogicalOperator {
public:
    LogicalCreateMacro(std::shared_ptr<binder::Expression> outputExpression, std::string macroName,
        std::unique_ptr<function::ScalarMacroFunction> macro)
        : LogicalOperator{LogicalOperatorType::CREATE_MACRO}, outputExpression{std::move(
                                                                  outputExpression)},
          macroName{std::move(macroName)}, macro{std::move(macro)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    inline std::string getMacroName() const { return macroName; }

    inline std::unique_ptr<function::ScalarMacroFunction> getMacro() const { return macro->copy(); }

    inline std::string getExpressionsForPrinting() const override { return macroName; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalCreateMacro>(outputExpression, macroName, macro->copy());
    }

private:
    std::shared_ptr<binder::Expression> outputExpression;
    std::string macroName;
    std::shared_ptr<function::ScalarMacroFunction> macro;
};

} // namespace planner
} // namespace kuzu
