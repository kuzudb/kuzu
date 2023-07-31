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

    CatalogContent(const CatalogContent& other);

    /**
     * Node and Rel table functions.
     */
    common::table_id_t addNodeTableSchema(std::string tableName, common::property_id_t primaryKeyId,
        std::vector<Property> properties);

    common::table_id_t addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
        std::vector<Property> properties, common::table_id_t srcTableID,
        common::table_id_t dstTableID, common::LogicalType srcPKDataType,
        common::LogicalType dstPKDataType);

    inline bool hasNodeTable() const { return !nodeTableSchemas.empty(); }
    inline bool hasRelTable() const { return !relTableSchemas.empty(); }

    inline bool containNodeTable(common::table_id_t tableID) const {
        return nodeTableSchemas.contains(tableID);
    }
    inline bool containRelTable(common::table_id_t tableID) const {
        return relTableSchemas.contains(tableID);
    }
    inline bool containTable(const std::string& name) const {
        return containNodeTable(name) || containRelTable(name);
    }

    inline std::string getTableName(common::table_id_t tableID) const {
        return getTableSchema(tableID)->tableName;
    }

    inline NodeTableSchema* getNodeTableSchema(common::table_id_t tableID) const {
        assert(containNodeTable(tableID));
        return nodeTableSchemas.at(tableID).get();
    }
    inline RelTableSchema* getRelTableSchema(common::table_id_t tableID) const {
        assert(containRelTable(tableID));
        return relTableSchemas.at(tableID).get();
    }
    inline TableSchema* getTableSchema(common::table_id_t tableID) const {
        assert(containRelTable(tableID) || containNodeTable(tableID));
        return nodeTableSchemas.contains(tableID) ?
                   (TableSchema*)nodeTableSchemas.at(tableID).get() :
                   (TableSchema*)relTableSchemas.at(tableID).get();
    }

    inline bool containNodeTable(const std::string& tableName) const {
        return nodeTableNameToIDMap.contains(tableName);
    }
    inline bool containRelTable(const std::string& tableName) const {
        return relTableNameToIDMap.contains(tableName);
    }

    inline common::table_id_t getTableID(const std::string& tableName) const {
        return nodeTableNameToIDMap.contains(tableName) ? nodeTableNameToIDMap.at(tableName) :
                                                          relTableNameToIDMap.at(tableName);
    }
    inline bool isSingleMultiplicityInDirection(
        common::table_id_t tableID, common::RelDataDirection direction) const {
        return relTableSchemas.at(tableID)->isSingleMultiplicityInDirection(direction);
    }

    /**
     * Node and Rel property functions.
     */
    // getNodeProperty and getRelProperty should be called after checking if property exists
    // (containNodeProperty and containRelProperty).
    const Property& getNodeProperty(
        common::table_id_t tableID, const std::string& propertyName) const;
    const Property& getRelProperty(
        common::table_id_t tableID, const std::string& propertyName) const;

    inline const std::vector<Property>& getNodeProperties(common::table_id_t tableID) const {
        return nodeTableSchemas.at(tableID)->getProperties();
    }
    inline const std::vector<Property>& getRelProperties(common::table_id_t tableID) const {
        return relTableSchemas.at(tableID)->getProperties();
    }
    inline std::vector<common::table_id_t> getNodeTableIDs() const {
        std::vector<common::table_id_t> nodeTableIDs;
        for (auto& [tableID, _] : nodeTableSchemas) {
            nodeTableIDs.push_back(tableID);
        }
        return nodeTableIDs;
    }
    inline std::vector<common::table_id_t> getRelTableIDs() const {
        std::vector<common::table_id_t> relTableIDs;
        for (auto& [tableID, _] : relTableSchemas) {
            relTableIDs.push_back(tableID);
        }
        return relTableIDs;
    }
    inline std::unordered_map<common::table_id_t, std::unique_ptr<NodeTableSchema>>&
    getNodeTableSchemas() {
        return nodeTableSchemas;
    }
    inline std::unordered_map<common::table_id_t, std::unique_ptr<RelTableSchema>>&
    getRelTableSchemas() {
        return relTableSchemas;
    }

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

private:
    inline common::table_id_t assignNextTableID() { return nextTableID++; }

    static void validateStorageVersion(storage::storage_version_t savedStorageVersion);

    static void validateMagicBytes(common::FileInfo* fileInfo, common::offset_t& offset);

    static void writeMagicBytes(common::FileInfo* fileInfo, common::offset_t& offset);

    void registerBuiltInFunctions();

private:
    // TODO(Guodong): I don't think it's necessary to keep separate maps for node and rel tables.
    std::unordered_map<common::table_id_t, std::unique_ptr<NodeTableSchema>> nodeTableSchemas;
    std::unordered_map<common::table_id_t, std::unique_ptr<RelTableSchema>> relTableSchemas;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    std::unordered_map<std::string, common::table_id_t> nodeTableNameToIDMap;
    std::unordered_map<std::string, common::table_id_t> relTableNameToIDMap;
    common::table_id_t nextTableID;
    std::unique_ptr<function::BuiltInVectorFunctions> builtInVectorFunctions;
    std::unique_ptr<function::BuiltInAggregateFunctions> builtInAggregateFunctions;
    std::unique_ptr<function::BuiltInTableFunctions> builtInTableFunctions;
    std::unordered_map<std::string, std::unique_ptr<function::ScalarMacroFunction>> macros;
};

} // namespace catalog
} // namespace kuzu
