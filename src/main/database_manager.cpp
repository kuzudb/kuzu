#include "main/database_manager.h"

#include "common/string_utils.h"

namespace kuzu {
namespace main {

void DatabaseManager::registerAttachedDatabase(std::unique_ptr<AttachedDatabase> attachedDatabase) {
    attachedDatabases.push_back(std::move(attachedDatabase));
}

AttachedDatabase* DatabaseManager::getAttachedDatabase(const std::string& name) {
    auto upperCaseName = common::StringUtils::getUpper(name);
    for (auto& attachedDatabase : attachedDatabases) {
        auto attachedDBName = attachedDatabase->getDBName();
        common::StringUtils::toUpper(attachedDBName);
        if (attachedDBName == upperCaseName) {
            return attachedDatabase.get();
        }
    }
    return nullptr;
}

void DatabaseManager::detachDatabase(const std::string& databaseName) {
    auto upperCaseName = common::StringUtils::getUpper(databaseName);
    for (auto it = attachedDatabases.begin(); it != attachedDatabases.end(); ++it) {
        auto attachedDBName = (*it)->getDBName();
        common::StringUtils::toUpper(attachedDBName);
        if (attachedDBName == upperCaseName) {
            attachedDatabases.erase(it);
            return;
        }
    }
    KU_UNREACHABLE;
}

} // namespace main
} // namespace kuzu
