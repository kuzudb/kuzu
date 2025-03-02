#include "processor/operator/ddl/create_type.h"

#include "catalog/catalog.h"
#include "processor/execution_context.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateTypePrintInfo::toString() const {
    return typeName + " AS " + type;
}

void CreateType::executeInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    catalog->createType(context->clientContext->getTransaction(), name, type.copy());
}

std::string CreateType::getOutputMsg() {
    return stringFormat("Type {}({}) has been created.", name, type.toString());
}

} // namespace processor
} // namespace kuzu
