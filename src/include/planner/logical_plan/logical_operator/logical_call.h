#pragma once

#include "base_logical_operator.h"
#include "main/db_config.h"

namespace kuzu {
namespace planner {

class LogicalCall : public LogicalOperator {
public:
    LogicalCall(main::ConfigurationOption option, std::shared_ptr<binder::Expression> optionValue)
        : LogicalOperator{LogicalOperatorType::CALL}, option{std::move(option)},
          optionValue{std::move(optionValue)} {}

    inline main::ConfigurationOption getOption() const { return option; }
    inline std::shared_ptr<binder::Expression> getOptionValue() const { return optionValue; }

    inline std::string getExpressionsForPrinting() const override { return option.name; }

    inline void computeFlatSchema() override { createEmptySchema(); }

    inline void computeFactorizedSchema() override { createEmptySchema(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCall>(option, optionValue);
    }

protected:
    main::ConfigurationOption option;
    std::shared_ptr<binder::Expression> optionValue;
};

} // namespace planner
} // namespace kuzu
