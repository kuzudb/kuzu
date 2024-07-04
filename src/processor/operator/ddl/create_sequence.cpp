#include "processor/operator/ddl/create_sequence.h"

#include "catalog/catalog.h"
#include "common/string_format.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateSequencePrintInfo::toString() const {
    return seqName;
}

void CreateSequence::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    switch (info.onConflict) {
    case common::ConflictAction::ON_CONFLICT_DO_NOTHING: {
        if (catalog->containsSequence(context->clientContext->getTx(), info.sequenceName)) {
            return;
        }
    }
    default:
        break;
    }
    catalog->createSequence(context->clientContext->getTx(), info);
}

std::string CreateSequence::getOutputMsg() {
    switch (info.onConflict) {
    case common::ConflictAction::ON_CONFLICT_THROW:
        return stringFormat("Sequence {} has been created.", info.sequenceName);
    case common::ConflictAction::ON_CONFLICT_DO_NOTHING:
        return stringFormat("Sequence {} already exists.", info.sequenceName);
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu
