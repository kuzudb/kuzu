#pragma once

#include <memory>
#include <string>

#include "catalog/catalog_content.h"

namespace kuzu {
namespace main {

class AttachedDatabase {
public:
    AttachedDatabase(std::string dbName, std::unique_ptr<catalog::CatalogContent> catalogContent)
        : dbName{std::move(dbName)}, catalogContent{std::move(catalogContent)} {}

    std::string getDBName() { return dbName; }

    catalog::CatalogContent* getCatalogContent() { return catalogContent.get(); }

private:
    std::string dbName;
    std::unique_ptr<catalog::CatalogContent> catalogContent;
};

} // namespace main
} // namespace kuzu
