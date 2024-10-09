#include "processor/operator/ddl/drop.h"

#include "catalog/catalog.h"
#include "common/string_format.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool isValidEntry(parser::DropInfo& dropInfo, main::ClientContext* context) {
    bool validEntry = false;
    switch (dropInfo.dropType) {
    case common::DropType::SEQUENCE: {
        validEntry = context->getCatalog()->containsSequence(context->getTx(), dropInfo.name);
    } break;
        // TODO(Ziyi): If the table has indexes, we should drop those indexes as well.
    case common::DropType::TABLE: {
        validEntry = context->getCatalog()->containsTable(context->getTx(), dropInfo.name);
    } break;
    default:
        KU_UNREACHABLE;
    }
    return validEntry;
}

void Drop::executeDDLInternal(ExecutionContext* context) {
    validEntry = isValidEntry(dropInfo, context->clientContext);
    if (!validEntry) {
        return;
    }
    switch (dropInfo.dropType) {
    case common::DropType::SEQUENCE: {
        context->clientContext->getCatalog()->dropSequence(context->clientContext->getTx(),
            dropInfo.name);
    } break;
    case common::DropType::TABLE: {
        context->clientContext->getCatalog()->dropTableEntry(context->clientContext->getTx(),
            dropInfo.name);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

std::string Drop::getOutputMsg() {
    if (validEntry) {
        return stringFormat("{} {} has been dropped.", DropTypeUtils::toString(dropInfo.dropType),
            dropInfo.name);
    } else {
        return stringFormat("{} {} does not exist.", DropTypeUtils::toString(dropInfo.dropType),
            dropInfo.name);
    }
}

} // namespace processor
} // namespace kuzu
