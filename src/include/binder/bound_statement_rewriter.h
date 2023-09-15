#pragma once

#include "bound_statement.h"

namespace kuzu {
namespace binder {

class BoundStatementRewriter {
public:
    static void rewrite(BoundStatement& boundStatement);
};

} // namespace binder
} // namespace kuzu
