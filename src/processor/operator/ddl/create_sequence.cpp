#include "processor/operator/ddl/create_sequence.h"

#include "catalog/catalog.h"
#include "common/string_format.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateSequence::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    catalog->createSequence(context->clientContext->getTx(), info);
}

std::string CreateSequence::getOutputMsg() {
    return stringFormat("Sequence {} has been created.", info.sequenceName);
}

} // namespace processor
} // namespace kuzu
