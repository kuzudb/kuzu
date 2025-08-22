#include "processor/operator/ddl/create_type.h"

#include "catalog/catalog.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateTypePrintInfo::toString() const {
    return typeName + " AS " + type;
}

void CreateType::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    Catalog::Get(*clientContext)->createType(clientContext->getTransaction(), name, type.copy());
    appendMessage(stringFormat("Type {}({}) has been created.", name, type.toString()),
        storage::MemoryManager::Get(*clientContext));
}

} // namespace processor
} // namespace kuzu
