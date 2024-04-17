#pragma once

#include "attached_database.h"

namespace kuzu {
namespace main {

class DatabaseManager {
public:
    DatabaseManager();

    void registerAttachedDatabase(std::unique_ptr<AttachedDatabase> attachedDatabase);
    AttachedDatabase* getAttachedDatabase(const std::string& name);
    void detachDatabase(const std::string& databaseName);
    std::string getDefaultDatabase() const { return defaultDatabase; }
    bool hasDefaultDatabase() const { return defaultDatabase != ""; }
    void setDefaultDatabase(const std::string& databaseName);
    std::vector<AttachedDatabase*> getAttachedDatabases() const;
    KUZU_API void invalidateCache();

private:
    std::vector<std::unique_ptr<AttachedDatabase>> attachedDatabases;
    std::string defaultDatabase;
};

} // namespace main
} // namespace kuzu
