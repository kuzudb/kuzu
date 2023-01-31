#pragma once

#include <memory>
#include <utility>

#include "catalog_structs.h"
#include "common/assert.h"
#include "common/exception.h"
#include "common/file_utils.h"
#include "common/ser_deser.h"
#include "common/utils.h"
#include "function/aggregate/built_in_aggregate_functions.h"
#include "function/built_in_vector_operations.h"
#include "storage/wal/wal.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace catalog {

class CatalogContent {
public:
    // This constructor is only used for mock catalog testing only.
    CatalogContent();

    explicit CatalogContent(const std::string& directory);

    CatalogContent(const CatalogContent& other);

    virtual ~CatalogContent() = default;

    /**
     * Node and Rel table functions.
     */
    table_id_t addNodeTableSchema(std::string tableName, property_id_t primaryKeyId,
        std::vector<PropertyNameDataType> propertyDefinitions);

    table_id_t addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
        const std::vector<PropertyNameDataType>& propertyDefinitions, table_id_t srcTableID,
        table_id_t dstTableID);

    inline bool containNodeTable(table_id_t tableID) const {
        return nodeTableSchemas.contains(tableID);
    }
    inline bool containRelTable(table_id_t tableID) const {
        return relTableSchemas.contains(tableID);
    }
    inline bool containTable(const std::string& name) const {
        return containNodeTable(name) || containRelTable(name);
    }

    inline std::string getTableName(table_id_t tableID) const {
        return getTableSchema(tableID)->tableName;
    }

    inline NodeTableSchema* getNodeTableSchema(table_id_t tableID) const {
        assert(containNodeTable(tableID));
        return nodeTableSchemas.at(tableID).get();
    }
    inline RelTableSchema* getRelTableSchema(table_id_t tableID) const {
        assert(containRelTable(tableID));
        return relTableSchemas.at(tableID).get();
    }
    inline TableSchema* getTableSchema(table_id_t tableID) const {
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

    inline table_id_t getTableID(const std::string& tableName) const {
        return nodeTableNameToIDMap.contains(tableName) ? nodeTableNameToIDMap.at(tableName) :
                                                          relTableNameToIDMap.at(tableName);
    }
    inline bool isSingleMultiplicityInDirection(table_id_t tableID, RelDirection direction) const {
        return relTableSchemas.at(tableID)->isSingleMultiplicityInDirection(direction);
    }

    /**
     * Node and Rel property functions.
     */
    // getNodeProperty and getRelProperty should be called after checking if property exists
    // (containNodeProperty and containRelProperty).
    const Property& getNodeProperty(table_id_t tableID, const std::string& propertyName) const;
    const Property& getRelProperty(table_id_t tableID, const std::string& propertyName) const;

    std::vector<Property> getAllNodeProperties(table_id_t tableID) const;
    inline const std::vector<Property>& getRelProperties(table_id_t tableID) const {
        return relTableSchemas.at(tableID)->properties;
    }
    inline std::vector<table_id_t> getNodeTableIDs() const {
        std::vector<table_id_t> nodeTableIDs;
        for (auto& [tableID, _] : nodeTableSchemas) {
            nodeTableIDs.push_back(tableID);
        }
        return nodeTableIDs;
    }
    inline std::vector<table_id_t> getRelTableIDs() const {
        std::vector<table_id_t> relTableIDs;
        for (auto& [tableID, _] : relTableSchemas) {
            relTableIDs.push_back(tableID);
        }
        return relTableIDs;
    }
    inline std::unordered_map<table_id_t, std::unique_ptr<NodeTableSchema>>& getNodeTableSchemas() {
        return nodeTableSchemas;
    }
    inline std::unordered_map<table_id_t, std::unique_ptr<RelTableSchema>>& getRelTableSchemas() {
        return relTableSchemas;
    }

    void dropTableSchema(table_id_t tableID);

    void renameTable(table_id_t tableID, std::string newName);

    void saveToFile(const std::string& directory, common::DBFileType dbFileType);
    void readFromFile(const std::string& directory, common::DBFileType dbFileType);

private:
    inline table_id_t assignNextTableID() { return nextTableID++; }

private:
    std::shared_ptr<spdlog::logger> logger;
    std::unordered_map<table_id_t, std::unique_ptr<NodeTableSchema>> nodeTableSchemas;
    std::unordered_map<table_id_t, std::unique_ptr<RelTableSchema>> relTableSchemas;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    std::unordered_map<std::string, table_id_t> nodeTableNameToIDMap;
    std::unordered_map<std::string, table_id_t> relTableNameToIDMap;
    table_id_t nextTableID;
};

class Catalog {
public:
    Catalog();

    explicit Catalog(storage::WAL* wal);

    virtual ~Catalog() = default;

    inline CatalogContent* getReadOnlyVersion() const { return catalogContentForReadOnlyTrx.get(); }

    inline CatalogContent* getWriteVersion() const { return catalogContentForWriteTrx.get(); }

    inline function::BuiltInVectorOperations* getBuiltInScalarFunctions() const {
        return builtInVectorOperations.get();
    }
    inline function::BuiltInAggregateFunctions* getBuiltInAggregateFunction() const {
        return builtInAggregateFunctions.get();
    }

    inline bool hasUpdates() { return catalogContentForWriteTrx != nullptr; }

    void checkpointInMemoryIfNecessary();

    inline void initCatalogContentForWriteTrxIfNecessary() {
        if (!catalogContentForWriteTrx) {
            catalogContentForWriteTrx =
                std::make_unique<CatalogContent>(*catalogContentForReadOnlyTrx);
        }
    }

    inline void writeCatalogForWALRecord(const std::string& directory) {
        catalogContentForWriteTrx->saveToFile(directory, common::DBFileType::WAL_VERSION);
    }

    static inline void saveInitialCatalogToFile(const std::string& directory) {
        std::make_unique<Catalog>()->getReadOnlyVersion()->saveToFile(
            directory, common::DBFileType::ORIGINAL);
    }

    common::ExpressionType getFunctionType(const std::string& name) const;

    table_id_t addNodeTableSchema(std::string tableName, property_id_t primaryKeyId,
        std::vector<PropertyNameDataType> propertyDefinitions);

    table_id_t addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
        const std::vector<PropertyNameDataType>& propertyDefinitions, table_id_t srcTableID,
        table_id_t dstTableID);

    void dropTableSchema(table_id_t tableID);

    void renameTable(table_id_t tableID, std::string newName);

    void addProperty(table_id_t tableID, std::string propertyName, DataType dataType);

    void dropProperty(table_id_t tableID, property_id_t propertyID);

    void renameProperty(table_id_t tableID, property_id_t propertyID, std::string newName);

    std::unordered_set<RelTableSchema*> getAllRelTableSchemasContainBoundTable(
        table_id_t boundTableID);

protected:
    std::unique_ptr<function::BuiltInVectorOperations> builtInVectorOperations;
    std::unique_ptr<function::BuiltInAggregateFunctions> builtInAggregateFunctions;
    std::unique_ptr<CatalogContent> catalogContentForReadOnlyTrx;
    std::unique_ptr<CatalogContent> catalogContentForWriteTrx;
    storage::WAL* wal;
};

} // namespace catalog
} // namespace kuzu
