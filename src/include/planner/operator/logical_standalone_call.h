#pragma once

#include "main/db_config.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalStandaloneCall final : public LogicalOperator {
public:
    LogicalStandaloneCall(const main::Option* option,
        std::shared_ptr<binder::Expression> optionValue)
        : LogicalOperator{LogicalOperatorType::STANDALONE_CALL}, option{option},
          optionValue{std::move(optionValue)} {}

    const main::Option* getOption() const { return option; }
    std::shared_ptr<binder::Expression> getOptionValue() const { return optionValue; }

    std::string getExpressionsForPrinting() const override { return option->name; }

    void computeFlatSchema() override { createEmptySchema(); }

    void computeFactorizedSchema() override { createEmptySchema(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalStandaloneCall>(option, optionValue);
    }

protected:
    const main::Option* option;
    std::shared_ptr<binder::Expression> optionValue;
};

} // namespace planner
} // namespace kuzu
