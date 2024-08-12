#include "binder/binder.h"
#include "main/database_manager.h"
#include "common/exception/binder.h"
#include "main/client_context.h"
#include "catalog/catalog_entry/external_node_table_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

catalog::TableCatalogEntry* Binder::bindExternalTableEntry(const std::string& dbName,
    const std::string& tableName) {
    auto attachedDB = clientContext->getDatabaseManager()->getAttachedDatabase(dbName);
    if (attachedDB == nullptr) {
        throw BinderException{stringFormat("No database named {} has been attached.", dbName)};
    }
    auto attachedCatalog = attachedDB->getCatalog();
    auto tableID = attachedCatalog->getTableID(clientContext->getTx(), tableName);
    return attachedCatalog->getTableCatalogEntry(clientContext->getTx(), tableID);
}

void Binder::bindExternalTableEntry(NodeOrRelExpression& nodeOrRel) {
    if (nodeOrRel.isMultiLabeled() || nodeOrRel.isEmpty()) {
        return ;
    }
    auto entry = nodeOrRel.getSingleEntry();
    switch (entry->getType()) {
    case CatalogEntryType::EXTERNAL_NODE_TABLE_ENTRY: {
        auto& tableEntry = entry->constCast<ExternalNodeTableCatalogEntry>();
        auto externalEntry = bindExternalTableEntry(tableEntry.getExternalDBName(), tableEntry.getExternalTableName());
        nodeOrRel.setExternalEntry(externalEntry);
    } break ;
    default:
        break;
    }
}

}
}
