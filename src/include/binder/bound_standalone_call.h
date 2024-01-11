#pragma once

#include <utility>

#include "binder/bound_statement.h"
#include "binder/expression/expression.h"
#include "main/db_config.h"

namespace kuzu {
namespace binder {

class BoundStandaloneCall : public BoundStatement {
public:
    BoundStandaloneCall(main::Option* option, std::shared_ptr<Expression> optionValue)
        : BoundStatement{common::StatementType::STANDALONE_CALL,
              BoundStatementResult::createEmptyResult()},
          option{option}, optionValue{std::move(optionValue)} {}

    inline main::Option* getOption() const { return option; }

    inline std::shared_ptr<Expression> getOptionValue() const { return optionValue; }

private:
    main::Option* option;
    std::shared_ptr<Expression> optionValue;
};

} // namespace binder
} // namespace kuzu
