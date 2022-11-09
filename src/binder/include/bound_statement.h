#pragma once
#include <cstdint>

#include "src/common/include/statement_type.h"

using namespace graphflow::common;

namespace graphflow {
namespace binder {

class BoundStatement {
public:
    explicit BoundStatement(StatementType statementType) : statementType{statementType} {}

    inline StatementType getStatementType() const { return statementType; }

private:
    StatementType statementType;
};

} // namespace binder
} // namespace graphflow
