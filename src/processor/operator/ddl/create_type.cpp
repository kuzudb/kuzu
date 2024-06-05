#include "processor/operator/ddl/create_type.h"

#include "catalog/catalog.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateType::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    catalog->createType(context->clientContext->getTx(), name, type);
}

std::string CreateType::getOutputMsg() {
    return stringFormat("Type {}({}) has been created.", name, type.toString());
}

} // namespace processor
} // namespace kuzu
