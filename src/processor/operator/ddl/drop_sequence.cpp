#include "processor/operator/ddl/drop_sequence.h"

#include "catalog/catalog.h"
#include "common/string_format.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {


void DropSequence::executeDDLInternal(ExecutionContext* context) {
    context->clientContext->getCatalog()->dropSequence(context->clientContext->getTx(), sequenceID);
}

std::string DropSequence::getOutputMsg() {
    return stringFormat("Sequence: {} has been dropped.", sequenceName);
}

std::string DropSequencePrintInfo::toString() const{
    std::string result = "Sequence Name: ";
    result += sequenceName;
    return result;
}

} // namespace processor
} // namespace kuzu
