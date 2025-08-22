#include "processor/operator/ddl/create_sequence.h"

#include "catalog/catalog.h"
#include "common/string_format.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateSequencePrintInfo::toString() const {
    return seqName;
}

void CreateSequence::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto catalog = Catalog::Get(*clientContext);
    auto transaction = clientContext->getTransaction();
    auto memoryManager = storage::MemoryManager::Get(*clientContext);
    if (catalog->containsSequence(transaction, info.sequenceName)) {
        switch (info.onConflict) {
        case ConflictAction::ON_CONFLICT_DO_NOTHING: {
            appendMessage(stringFormat("Sequence {} already exists.", info.sequenceName),
                memoryManager);
            return;
        }
        default:
            break;
        }
    }
    catalog->createSequence(transaction, info);
    appendMessage(stringFormat("Sequence {} has been created.", info.sequenceName), memoryManager);
}

} // namespace processor
} // namespace kuzu
