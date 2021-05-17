#pragma once

#include "src/common/include/statement_type.h"

using namespace graphflow::common;

namespace graphflow {
namespace parser {

class ReadingStatement {

protected:
    explicit ReadingStatement(StatementType statementType) : statementType{statementType} {}

public:
    StatementType statementType;
};

} // namespace parser
} // namespace graphflow
