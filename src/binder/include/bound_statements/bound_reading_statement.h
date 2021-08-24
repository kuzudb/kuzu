#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/common/include/statement_type.h"

using namespace graphflow::common;

namespace graphflow {
namespace binder {

class BoundReadingStatement {

public:
    virtual ~BoundReadingStatement() = default;

    virtual vector<shared_ptr<Expression>> getIncludedVariables() const = 0;

protected:
    explicit BoundReadingStatement(StatementType statementType) : statementType{statementType} {}

public:
    StatementType statementType;
};

} // namespace binder
} // namespace graphflow
