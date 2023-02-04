#include "main/prepared_statement.h"

#include "binder/binder.h"
#include "optimizer/optimizer.h"

namespace kuzu {
namespace main {

expression_vector PreparedStatement::getExpressionsToCollect() {
    return statementResult->getExpressionsToCollect();
}

} // namespace main
} // namespace kuzu
