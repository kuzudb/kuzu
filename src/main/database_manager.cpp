#include "main/database_manager.h"

#include "common/string_utils.h"

namespace kuzu {
namespace main {

void DatabaseManager::registerAttachedDatabase(std::unique_ptr<AttachedDatabase> attachedDatabase) {
    attachedDatabases.push_back(std::move(attachedDatabase));
}

AttachedDatabase* DatabaseManager::getAttachedDatabase(const std::string& name) {
    for (auto& attachedDatabase : attachedDatabases) {
        if (attachedDatabase->getDBName() == name) {
            return attachedDatabase.get();
        }
    }
    return nullptr;
}

void DatabaseManager::detachDatabase(const std::string& databaseName) {
    auto upperCaseDBName = databaseName;
    common::StringUtils::toUpper(upperCaseDBName);
    for (auto it = attachedDatabases.begin(); it != attachedDatabases.end(); ++it) {
        auto attachedDBName = (*it)->getDBName();
        common::StringUtils::toUpper(attachedDBName);
        if (attachedDBName == upperCaseDBName) {
            attachedDatabases.erase(it);
            return;
        }
    }
    KU_UNREACHABLE;
}

} // namespace main
} // namespace kuzu
