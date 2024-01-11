#pragma once

#include "main/db_config.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalStandaloneCall : public LogicalOperator {
public:
    LogicalStandaloneCall(main::Option* option, std::shared_ptr<binder::Expression> optionValue)
        : LogicalOperator{LogicalOperatorType::STANDALONE_CALL}, option{option},
          optionValue{std::move(optionValue)} {}

    inline main::Option* getOption() const { return option; }
    inline std::shared_ptr<binder::Expression> getOptionValue() const { return optionValue; }

    inline std::string getExpressionsForPrinting() const override { return option->name; }

    inline void computeFlatSchema() override { createEmptySchema(); }

    inline void computeFactorizedSchema() override { createEmptySchema(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalStandaloneCall>(option, optionValue);
    }

protected:
    main::Option* option;
    std::shared_ptr<binder::Expression> optionValue;
};

} // namespace planner
} // namespace kuzu
