#include "binder/visitor/confidential_statement_analyzer.h"

#include "binder/bound_standalone_call.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

void ConfidentialStatementAnalyzer::visitStandaloneCall(const BoundStatement& boundStatement) {
    auto& standaloneCall = boundStatement.constCast<BoundStandaloneCall>();
    confidentialStatement = standaloneCall.getOption()->isConfidential;
}

} // namespace binder
} // namespace kuzu
