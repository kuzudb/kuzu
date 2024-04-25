#pragma once

#include <memory>
#include <string>

#include "extension/catalog_extension.h"

namespace kuzu {
namespace main {

class AttachedDatabase {
public:
    AttachedDatabase(std::string dbName, std::string dbType,
        std::unique_ptr<extension::CatalogExtension> catalog)
        : dbName{std::move(dbName)}, dbType{std::move(dbType)}, catalog{std::move(catalog)} {}

    std::string getDBName() const { return dbName; }

    std::string getDBType() const { return dbType; }

    extension::CatalogExtension* getCatalog() { return catalog.get(); }

    void invalidateCache() { catalog->invalidateCache(); }

private:
    std::string dbName;
    std::string dbType;
    std::unique_ptr<extension::CatalogExtension> catalog;
};

} // namespace main
} // namespace kuzu
