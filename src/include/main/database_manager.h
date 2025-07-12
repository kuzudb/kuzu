#pragma once

#include "database_instance.h"

namespace kuzu {
namespace main {

class DatabaseManager {
public:
    DatabaseManager();

    void registerAttachedDatabase(std::unique_ptr<DatabaseInstance> attachedDatabase);
    bool hasAttachedDatabase(const std::string& name) const;
    KUZU_API DatabaseInstance* getAttachedDatabase(const std::string& name);
    void detachDatabase(const std::string& databaseName);
    std::string getDefaultDatabase() const { return defaultDatabase; }
    DatabaseInstance* getDefaultDatabaseInstance();
    bool hasDefaultDatabase() const { return defaultDatabase != ""; }
    void setDefaultDatabase(const std::string& databaseName);
    std::vector<DatabaseInstance*> getAttachedDatabases() const;
    KUZU_API void invalidateCache();

private:
    std::vector<std::unique_ptr<DatabaseInstance>> attachedDatabases;
    std::string defaultDatabase;
};

} // namespace main
} // namespace kuzu
