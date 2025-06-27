#include "processor/operator/ddl/create_table.h"

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "processor/execution_context.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateTable::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    auto memoryManager = clientContext->getMemoryManager();
    // Check conflict
    if (catalog->containsTable(transaction, info.tableName)) {
        switch (info.onConflict) {
        case ConflictAction::ON_CONFLICT_DO_NOTHING: {
            appendMessage(stringFormat("Table {} already exists.", info.tableName), memoryManager);
            return;
        }
        case ConflictAction::ON_CONFLICT_THROW: {
            throw BinderException(info.tableName + " already exists in catalog.");
        }
        default:
            KU_UNREACHABLE;
        }
    }
    // Create the table.
    CatalogEntry* entry = nullptr;
    switch (info.type) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY: {
        entry = catalog->createTableEntry(transaction, info);
    } break;
    default:
        KU_UNREACHABLE;
    }
    clientContext->getStorageManager()->createTable(entry->ptrCast<TableCatalogEntry>());
    appendMessage(stringFormat("Table {} has been created.", info.tableName), memoryManager);
    sharedState->tableCreated = true;
}

} // namespace processor
} // namespace kuzu
