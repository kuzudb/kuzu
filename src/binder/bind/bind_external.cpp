#include "binder/binder.h"
#include "main/database_manager.h"
#include "common/exception/binder.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

catalog::CatalogEntry* Binder::bindExternalTableEntry(std::string dbName, std::string tableName) {
    auto attachedDB = clientContext->getDatabaseManager()->getAttachedDatabase(dbName);
    if (attachedDB == nullptr) {
        throw BinderException{stringFormat("No database named {} has been attached.", dbName)};
    }
    auto attachedCatalog = attachedDB->getCatalog();
    auto tableID = attachedCatalog->getTableID(clientContext->getTx(), tableName);
    return attachedCatalog->getTableCatalogEntry(clientContext->getTx(), tableID);
}

}
}
