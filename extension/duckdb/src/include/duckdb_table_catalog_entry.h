#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "function/table_functions.h"

namespace kuzu {
namespace catalog {

class DuckDBTableCatalogEntry final : public TableCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    DuckDBTableCatalogEntry(std::string name, common::table_id_t tableID,
        function::TableFunction scanFunction);

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    common::TableType getTableType() const override;
    function::TableFunction getScanFunction() override { return scanFunction; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    std::unique_ptr<CatalogEntry> copy() const override;

private:
    function::TableFunction scanFunction;
};

} // namespace catalog
} // namespace kuzu
