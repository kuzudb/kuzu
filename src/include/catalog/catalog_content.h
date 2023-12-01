#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "function/built_in_function.h"
#include "function/scalar_macro_function.h"
#include "table_schema.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common
namespace catalog {

class CatalogContent {
    friend class Catalog;

public:
    CatalogContent();

    explicit CatalogContent(const std::string& directory);

    CatalogContent(
        std::unordered_map<common::table_id_t, std::unique_ptr<TableSchema>> tableSchemas,
        std::unordered_map<std::string, common::table_id_t> tableNameToIDMap,
        common::table_id_t nextTableID,
        std::unique_ptr<function::BuiltInFunctions> builtInFunctions,
        std::unordered_map<std::string, std::unique_ptr<function::ScalarMacroFunction>> macros)
        : tableSchemas{std::move(tableSchemas)}, tableNameToIDMap{std::move(tableNameToIDMap)},
          nextTableID{nextTableID}, builtInFunctions{std::move(builtInFunctions)}, macros{std::move(
                                                                                       macros)} {}

    void saveToFile(const std::string& directory, common::FileVersionType dbFileType);
    void readFromFile(const std::string& directory, common::FileVersionType dbFileType);

    std::unique_ptr<CatalogContent> copy() const;

private:
    // ----------------------------- Functions ----------------------------
    common::ExpressionType getFunctionType(const std::string& name) const;

    void registerBuiltInFunctions();

    inline bool containMacro(const std::string& macroName) const {
        return macros.contains(macroName);
    }
    void addFunction(std::string name, function::function_set definitions);
    void addScalarMacroFunction(
        std::string name, std::unique_ptr<function::ScalarMacroFunction> macro);

    // ----------------------------- Table Schemas ----------------------------
    inline common::table_id_t assignNextTableID() { return nextTableID++; }
    inline uint64_t getTableCount() const { return tableSchemas.size(); }

    bool containsTable(const std::string& tableName) const;
    bool containsTable(common::TableType tableType) const;

    std::string getTableName(common::table_id_t tableID) const;

    TableSchema* getTableSchema(common::table_id_t tableID) const;
    std::vector<TableSchema*> getTableSchemas(common::TableType tableType) const;

    common::table_id_t getTableID(const std::string& tableName) const;
    std::vector<common::table_id_t> getTableIDs(common::TableType tableType) const;

    common::table_id_t addNodeTableSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRelTableSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRelTableGroupSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRdfGraphSchema(const binder::BoundCreateTableInfo& info);
    void dropTableSchema(common::table_id_t tableID);
    void renameTable(common::table_id_t tableID, const std::string& newName);

private:
    // TODO(Guodong): I don't think it's necessary to keep separate maps for node and rel tables.
    std::unordered_map<common::table_id_t, std::unique_ptr<TableSchema>> tableSchemas;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    std::unordered_map<std::string, common::table_id_t> tableNameToIDMap;
    common::table_id_t nextTableID;
    std::unique_ptr<function::BuiltInFunctions> builtInFunctions;
    std::unordered_map<std::string, std::unique_ptr<function::ScalarMacroFunction>> macros;
};

} // namespace catalog
} // namespace kuzu
