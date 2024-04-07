#pragma once

#include "bound_statement.h"
#include "main/client_context.h"

namespace kuzu {
namespace binder {

// Perform semantic rewrite over bound statement.
class BoundStatementRewriter {
public:
    static void rewrite(BoundStatement& boundStatement, const main::ClientContext& clientContext);
};

} // namespace binder
} // namespace kuzu
