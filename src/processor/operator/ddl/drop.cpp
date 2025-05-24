#include "processor/operator/ddl/drop.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "processor/execution_context.h"
#include <catalog/catalog_entry/index_catalog_entry.h>

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Drop::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    switch (dropInfo.dropType) {
    case DropType::SEQUENCE: {
        dropSequence(clientContext);
    } break;
    case DropType::TABLE: {
        if (catalog->containsTable(transaction, dropInfo.name,
                clientContext->useInternalCatalogEntry())) {
            dropTable(clientContext);
            return;
        }
        switch (dropInfo.conflictAction) {
        case ConflictAction::ON_CONFLICT_DO_NOTHING: {
            return;
        }
        case ConflictAction::ON_CONFLICT_THROW: {
            throw BinderException("Table " + dropInfo.name + " does not exist.");
        }
        default:
            KU_UNREACHABLE;
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Drop::dropSequence(const main::ClientContext* context) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    if (!catalog->containsSequence(transaction, dropInfo.name)) {
        switch (dropInfo.conflictAction) {
        case ConflictAction::ON_CONFLICT_DO_NOTHING: {
            return;
        }
        case ConflictAction::ON_CONFLICT_THROW: {
            throw BinderException(
                common::stringFormat("Sequence {} does not exist.", dropInfo.name));
        }
        default:
            KU_UNREACHABLE;
        }
    }
    catalog->dropSequence(transaction, dropInfo.name);
    entryDropped = true;
}

void Drop::dropTable(const main::ClientContext* context) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    auto entry = catalog->getTableCatalogEntry(transaction, dropInfo.name);
    switch (entry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY: {
        for (auto& indexEntry : catalog->getIndexEntries(transaction)) {
            if (indexEntry->getTableID() == entry->getTableID()) {
                throw BinderException(stringFormat(
                    "Cannot delete node table {} because it is referenced by index {}.",
                    entry->getName(), indexEntry->getIndexName()));
            }
        }
        for (auto& relEntry : catalog->getRelGroupEntries(transaction)) {
            if (relEntry->isParent(entry->getTableID())) {
                throw BinderException(stringFormat("Cannot delete node table {} because it is "
                                                   "referenced by relationship table {}.",
                    entry->getName(), relEntry->getName()));
            }
        }
    } break;
    case CatalogEntryType::REL_GROUP_ENTRY: {
        // Do nothing
    } break;
    default:
        KU_UNREACHABLE;
    }
    catalog->dropTableEntryAndIndex(transaction, dropInfo.name);
    entryDropped = true;
}

std::string Drop::getOutputMsg() {
    if (entryDropped) {
        return stringFormat("{} {} has been dropped.", DropTypeUtils::toString(dropInfo.dropType),
            dropInfo.name);
    } else {
        return stringFormat("{} {} does not exist.", DropTypeUtils::toString(dropInfo.dropType),
            dropInfo.name);
    }
}

} // namespace processor
} // namespace kuzu
