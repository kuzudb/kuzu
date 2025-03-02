#include "processor/operator/ddl/drop.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "processor/execution_context.h"

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
        if (catalog->containsRelGroup(transaction, dropInfo.name)) {
            dropRelGroup(clientContext);
            return;
        }
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
        for (auto& relEntry : catalog->getRelTableEntries(transaction)) {
            if (relEntry->isParent(entry->getTableID())) {
                throw BinderException(stringFormat("Cannot delete node table {} because it is "
                                                   "referenced by relationship table {}.",
                    entry->getName(), relEntry->getName()));
            }
        }
    } break;
    case CatalogEntryType::REL_TABLE_ENTRY: {
        auto parentRelGroup =
            entry->ptrCast<RelTableCatalogEntry>()->getParentRelGroup(catalog, transaction);
        if (parentRelGroup != nullptr) {
            throw BinderException(stringFormat("Cannot delete relationship table {} because it "
                                               "is referenced by relationship group {}.",
                entry->getName(), parentRelGroup->getName()));
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    catalog->dropTableEntryAndIndex(transaction, dropInfo.name);
    entryDropped = true;
}

void Drop::dropRelGroup(const main::ClientContext* context) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    auto entry = catalog->getRelGroupEntry(transaction, dropInfo.name);
    catalog->dropRelGroupEntry(transaction, entry);
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
