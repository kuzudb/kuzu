#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "function/built_in_function.h"
#include "function/scalar_macro_function.h"
#include "storage/storage_info.h"
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

    /*
     * Single schema lookup.
     * */
    inline bool containsTable(const std::string& tableName) const {
        return tableNameToIDMap.contains(tableName);
    }
    inline bool containsNodeTable(const std::string& tableName) const {
        return containsTable(tableName, common::TableType::NODE);
    }
    inline bool containsRelTable(const std::string& tableName) const {
        return containsTable(tableName, common::TableType::REL);
    }
    inline std::string getTableName(common::table_id_t tableID) const {
        KU_ASSERT(tableSchemas.contains(tableID));
        return getTableSchema(tableID)->tableName;
    }
    inline TableSchema* getTableSchema(common::table_id_t tableID) const {
        KU_ASSERT(tableSchemas.contains(tableID));
        return tableSchemas.at(tableID).get();
    }
    inline common::table_id_t getTableID(const std::string& tableName) const {
        KU_ASSERT(tableNameToIDMap.contains(tableName));
        return tableNameToIDMap.at(tableName);
    }

    /*
     * Batch schema lookup.
     * */
    inline uint64_t getTableCount() const { return tableSchemas.size(); }
    inline bool containsNodeTable() const { return containsTable(common::TableType::NODE); }
    inline bool containsRelTable() const { return containsTable(common::TableType::REL); }
    inline bool containsRdfGraph() const { return containsTable(common::TableType::RDF); }
    inline std::vector<common::table_id_t> getNodeTableIDs() const {
        return getTableIDs(common::TableType::NODE);
    }
    inline std::vector<common::table_id_t> getRelTableIDs() const {
        return getTableIDs(common::TableType::REL);
    }
    inline std::vector<common::table_id_t> getRdfGraphIDs() const {
        return getTableIDs(common::TableType::RDF);
    }
    inline std::vector<TableSchema*> getNodeTableSchemas() const {
        return getTableSchemas(common::TableType::NODE);
    }
    inline std::vector<TableSchema*> getRelTableSchemas() const {
        return getTableSchemas(common::TableType::REL);
    }
    inline std::vector<TableSchema*> getRelTableGroupSchemas() const {
        return getTableSchemas(common::TableType::REL_GROUP);
    }

    std::vector<TableSchema*> getTableSchemas() const;
    std::vector<TableSchema*> getTableSchemas(
        const std::vector<common::table_id_t>& tableIDs) const;

    /**
     * Add schema.
     */
    common::table_id_t addNodeTableSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRelTableSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRelTableGroupSchema(const binder::BoundCreateTableInfo& info);
    common::table_id_t addRdfGraphSchema(const binder::BoundCreateTableInfo& info);

    inline bool containMacro(const std::string& macroName) const {
        return macros.contains(macroName);
    }

    void dropTableSchema(common::table_id_t tableID);

    void renameTable(common::table_id_t tableID, const std::string& newName);

    void saveToFile(const std::string& directory, common::FileVersionType dbFileType);
    void readFromFile(const std::string& directory, common::FileVersionType dbFileType);

    common::ExpressionType getFunctionType(const std::string& name) const;

    void addFunction(std::string name, function::function_set definitions);

    void addScalarMacroFunction(
        std::string name, std::unique_ptr<function::ScalarMacroFunction> macro);

    std::unique_ptr<CatalogContent> copy() const;

private:
    inline common::table_id_t assignNextTableID() { return nextTableID++; }

    static void validateStorageVersion(storage::storage_version_t savedStorageVersion);

    static void validateMagicBytes(common::Deserializer& deserializer);

    static void writeMagicBytes(common::Serializer& serializer);

    void registerBuiltInFunctions();

    bool containsTable(const std::string& tableName, common::TableType tableType) const;
    bool containsTable(common::TableType tableType) const;
    std::vector<TableSchema*> getTableSchemas(common::TableType tableType) const;
    std::vector<common::table_id_t> getTableIDs(common::TableType tableType) const;

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
