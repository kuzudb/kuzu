#pragma once

#include "catalog/table_schema.h"
#include "function/aggregate/built_in_aggregate_functions.h"
#include "function/built_in_table_functions.h"
#include "function/built_in_vector_functions.h"
#include "function/scalar_macro_function.h"
#include "storage/storage_info.h"

namespace kuzu {
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
        std::unordered_map<std::string, std::unique_ptr<function::ScalarMacroFunction>> macros)
        : tableSchemas{std::move(tableSchemas)}, tableNameToIDMap{std::move(tableNameToIDMap)},
          nextTableID{nextTableID}, macros{std::move(macros)} {
        registerBuiltInFunctions();
    }

    inline bool containTable(const std::string& name) const {
        return tableNameToIDMap.contains(name);
    }
    inline std::string getTableName(common::table_id_t tableID) const {
        assert(tableSchemas.contains(tableID));
        return getTableSchema(tableID)->tableName;
    }
    inline TableSchema* getTableSchema(common::table_id_t tableID) const {
        assert(tableSchemas.contains(tableID));
        return tableSchemas.at(tableID).get();
    }
    inline NodeTableSchema* getNodeTableSchema(common::table_id_t tableID) const {
        return reinterpret_cast<NodeTableSchema*>(getTableSchema(tableID));
    }
    inline RelTableSchema* getRelTableSchema(common::table_id_t tableID) const {
        return reinterpret_cast<RelTableSchema*>(getTableSchema(tableID));
    }
    inline common::table_id_t getTableID(const std::string& tableName) const {
        assert(tableNameToIDMap.contains(tableName));
        return tableNameToIDMap.at(tableName);
    }

    inline bool isSingleMultiplicityInDirection(
        common::table_id_t tableID, common::RelDataDirection direction) const {
        return getRelTableSchema(tableID)->isSingleMultiplicityInDirection(direction);
    }

    /**
     * Node and Rel table functions.
     */
    common::table_id_t addNodeTableSchema(std::string tableName, common::property_id_t primaryKeyId,
        std::vector<std::unique_ptr<Property>> properties);

    common::table_id_t addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
        std::vector<std::unique_ptr<Property>> properties, common::table_id_t srcTableID,
        common::table_id_t dstTableID, std::unique_ptr<common::LogicalType> srcPKDataType,
        std::unique_ptr<common::LogicalType> dstPKDataType);

    bool containNodeTable(const std::string& tableName) const;

    bool containRelTable(const std::string& tableName) const;

    /**
     * Node and Rel property functions.
     */
    // getNodeProperty and getRelProperty should be called after checking if property exists
    // (containNodeProperty and containRelProperty).
    Property* getNodeProperty(common::table_id_t tableID, const std::string& propertyName) const;
    Property* getRelProperty(common::table_id_t tableID, const std::string& propertyName) const;

    inline const std::vector<Property*> getProperties(common::table_id_t tableID) const {
        assert(tableSchemas.contains(tableID));
        return tableSchemas.at(tableID)->getProperties();
    }
    inline std::vector<common::table_id_t> getNodeTableIDs() const {
        return getTableIDsByType(TableType::NODE);
    }
    inline std::vector<common::table_id_t> getRelTableIDs() const {
        return getTableIDsByType(TableType::REL);
    }
    std::vector<NodeTableSchema*> getNodeTableSchemas() const;
    std::vector<RelTableSchema*> getRelTableSchemas() const;

    inline bool containMacro(const std::string& macroName) const {
        return macros.contains(macroName);
    }

    void dropTableSchema(common::table_id_t tableID);

    void renameTable(common::table_id_t tableID, const std::string& newName);

    void saveToFile(const std::string& directory, common::DBFileType dbFileType);
    void readFromFile(const std::string& directory, common::DBFileType dbFileType);

    common::ExpressionType getFunctionType(const std::string& name) const;

    void addVectorFunction(std::string name, function::vector_function_definitions definitions);

    void addScalarMacroFunction(
        std::string name, std::unique_ptr<function::ScalarMacroFunction> macro);

    std::unique_ptr<CatalogContent> copy() const;

private:
    inline common::table_id_t assignNextTableID() { return nextTableID++; }

    static void validateStorageVersion(storage::storage_version_t savedStorageVersion);

    static void validateMagicBytes(common::FileInfo* fileInfo, common::offset_t& offset);

    static void writeMagicBytes(common::FileInfo* fileInfo, common::offset_t& offset);

    void registerBuiltInFunctions();

    std::vector<common::table_id_t> getTableIDsByType(TableType tableType) const;

private:
    // TODO(Guodong): I don't think it's necessary to keep separate maps for node and rel tables.
    std::unordered_map<common::table_id_t, std::unique_ptr<TableSchema>> tableSchemas;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    std::unordered_map<std::string, common::table_id_t> tableNameToIDMap;
    common::table_id_t nextTableID;
    std::unique_ptr<function::BuiltInVectorFunctions> builtInVectorFunctions;
    std::unique_ptr<function::BuiltInAggregateFunctions> builtInAggregateFunctions;
    std::unique_ptr<function::BuiltInTableFunctions> builtInTableFunctions;
    std::unordered_map<std::string, std::unique_ptr<function::ScalarMacroFunction>> macros;
};

} // namespace catalog
} // namespace kuzu
