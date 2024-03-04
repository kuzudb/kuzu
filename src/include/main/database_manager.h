#pragma once

#include "attached_database.h"

namespace kuzu {
namespace main {

class DatabaseManager {
public:
    void registerAttachedDatabase(std::unique_ptr<AttachedDatabase> attachedDatabase);
    AttachedDatabase* getAttachedDatabase(const std::string& name);
    void detachDatabase(const std::string& databaseName);

private:
    std::vector<std::unique_ptr<AttachedDatabase>> attachedDatabases;
};

} // namespace main
} // namespace kuzu
