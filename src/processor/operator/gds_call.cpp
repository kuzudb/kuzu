#include "processor/operator/gds_call.h"

using namespace kuzu::binder;
using namespace kuzu::graph;

namespace kuzu {
namespace processor {

std::string GDSCallPrintInfo::toString() const {
    return "Algorithm: " + funcName;
}

void GDSCall::executeInternal(ExecutionContext* executionContext) {
    gds->exec(executionContext);
}

} // namespace processor
} // namespace kuzu
