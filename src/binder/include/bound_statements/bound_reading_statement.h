#pragma once

#include "src/common/include/statement_type.h"

using namespace graphflow::common;

namespace graphflow {
namespace binder {

class BoundReadingStatement {

protected:
    explicit BoundReadingStatement(StatementType statementType) : statementType{statementType} {}

public:
    StatementType statementType;
};

} // namespace binder
} // namespace graphflow
