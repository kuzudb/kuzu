#pragma once

#include <memory>
#include <string>

#include "extension/catalog_extension.h"

namespace kuzu {
namespace main {

class DatabaseInstance {
public:
    DatabaseInstance(std::string dbName, std::string dbType,
        std::unique_ptr<extension::CatalogExtension> catalog)
        : dbName{std::move(dbName)}, dbType{std::move(dbType)}, catalog{std::move(catalog)} {}

    virtual ~DatabaseInstance() = default;

    std::string getDBName() const { return dbName; }

    std::string getDBType() const { return dbType; }

    catalog::Catalog* getCatalog() const { return catalog.get(); }

    void invalidateCache();

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

protected:
    std::string dbName;
    std::string dbType;
    std::unique_ptr<catalog::Catalog> catalog;
};

} // namespace main
} // namespace kuzu
