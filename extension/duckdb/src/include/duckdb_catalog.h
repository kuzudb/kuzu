#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "common/vector/value_vector.h"
#include "extension/catalog_extension.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
// Supress warnings from duckdb.hpp
#undef ARROW_FLAG_DICTIONARY_ORDERED
#include "duckdb.hpp"
#pragma GCC diagnostic pop

namespace kuzu {
namespace binder {
struct AttachOption;
} // namespace binder

namespace duckdb_extension {

struct BoundExtraCreateDuckDBTableInfo : public binder::BoundExtraCreateTableInfo {
    std::string dbPath;
    std::string catalogName;
    std::string schemaName;

    BoundExtraCreateDuckDBTableInfo(std::string dbPath, std::string catalogName,
        std::string schemaName, std::vector<binder::PropertyInfo> propertyInfos)
        : BoundExtraCreateTableInfo{std::move(propertyInfos)}, dbPath{std::move(dbPath)},
          catalogName{std::move(catalogName)}, schemaName{std::move(schemaName)} {}
    BoundExtraCreateDuckDBTableInfo(const BoundExtraCreateDuckDBTableInfo& other)
        : BoundExtraCreateTableInfo{copyVector(other.propertyInfos)}, dbPath{other.dbPath},
          catalogName{other.catalogName}, schemaName{other.schemaName} {}

    std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
        return std::make_unique<BoundExtraCreateDuckDBTableInfo>(*this);
    }
};

class DuckDBCatalog : public extension::CatalogExtension {
public:
    DuckDBCatalog(std::string dbPath, std::string catalogName, main::ClientContext* context,
        const binder::AttachOption& attachOption);

    void init() override;

protected:
    bool bindPropertyInfos(duckdb::Connection& con, const std::string& tableName,
        const std::string& catalogName, std::vector<binder::PropertyInfo>& propertyInfos);

private:
    virtual std::unique_ptr<binder::BoundCreateTableInfo> bindCreateTableInfo(
        duckdb::Connection& con, const std::string& tableName, const std::string& dbPath,
        const std::string& catalogName);

    virtual std::string getDefaultSchemaName() const;

    virtual std::pair<duckdb::DuckDB, duckdb::Connection> getConnection(
        const std::string& dbPath) const;

private:
    void createForeignTable(duckdb::Connection& con, const std::string& tableName,
        const std::string& dbPath, const std::string& catalogName);

private:
    std::string dbPath;
    std::string catalogName;
    common::ValueVector tableNamesVector;
    bool skipUnsupportedTable;
};

} // namespace duckdb_extension
} // namespace kuzu
