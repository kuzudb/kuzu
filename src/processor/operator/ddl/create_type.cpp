#include "processor/operator/ddl/create_type.h"

#include "catalog/catalog.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateTypePrintInfo::toString() const {
    return typeName + " AS " + type;
}

void CreateType::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    catalog->createType(context->clientContext->getTx(), name, type.copy());
}

std::string CreateType::getOutputMsg() {
    return stringFormat("Type {}({}) has been created.", name, type.toString());
}

} // namespace processor
} // namespace kuzu
