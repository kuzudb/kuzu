#include "processor/operator/ddl/drop_sequence.h"

#include "catalog/catalog.h"
#include "common/string_format.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string DropSequencePrintInfo::toString() const {
    return name;
}

void DropSequence::executeDDLInternal(ExecutionContext* context) {
    context->clientContext->getCatalog()->dropSequence(context->clientContext->getTx(), sequenceID);
}

std::string DropSequence::getOutputMsg() {
    return stringFormat("Sequence: {} has been dropped.", sequenceName);
}

} // namespace processor
} // namespace kuzu
