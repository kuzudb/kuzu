#pragma once

#include "common/statement_type.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

class Statement {
public:
    explicit Statement(StatementType statementType) : statementType{statementType} {}

    virtual ~Statement() = default;

    inline StatementType getStatementType() const { return statementType; }

private:
    StatementType statementType;
};

} // namespace parser
} // namespace kuzu
