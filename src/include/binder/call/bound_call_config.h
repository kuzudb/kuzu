#pragma once

#include "binder/expression/expression.h"
#include "main/db_config.h"

namespace kuzu {
namespace binder {

class BoundCallConfig : public BoundStatement {
public:
    BoundCallConfig(main::ConfigurationOption option, std::shared_ptr<Expression> optionValue)
        : BoundStatement{common::StatementType::CALL_CONFIG,
              BoundStatementResult::createEmptyResult()},
          option{option}, optionValue{optionValue} {}

    inline main::ConfigurationOption getOption() const { return option; }

    inline std::shared_ptr<Expression> getOptionValue() const { return optionValue; }

private:
    main::ConfigurationOption option;
    std::shared_ptr<Expression> optionValue;
};

} // namespace binder
} // namespace kuzu
