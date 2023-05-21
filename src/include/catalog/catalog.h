#pragma once

#include <memory>

#include "catalog_structs.h"
#include "common/assert.h"
#include "common/exception.h"
#include "common/file_utils.h"
#include "common/ser_deser.h"
#include "common/utils.h"
#include "function/aggregate/built_in_aggregate_functions.h"
#include "function/built_in_vector_operations.h"
#include "storage/storage_info.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

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
    common::table_id_t addNodeTableSchema(std::string tableName, common::property_id_t primaryKeyId,
        std::vector<Property> properties);

    common::table_id_t addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
        std::vector<Property> properties, common::table_id_t srcTableID,
        common::table_id_t dstTableID);

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

    std::vector<Property> getAllNodeProperties(common::table_id_t tableID) const;
    inline const std::vector<Property>& getRelProperties(common::table_id_t tableID) const {
        return relTableSchemas.at(tableID)->properties;
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

    void dropTableSchema(common::table_id_t tableID);

    void renameTable(common::table_id_t tableID, const std::string& newName);

    void saveToFile(const std::string& directory, common::DBFileType dbFileType);
    void readFromFile(const std::string& directory, common::DBFileType dbFileType);

private:
    inline common::table_id_t assignNextTableID() { return nextTableID++; }

    void validateStorageVersion(storage::storage_version_t savedStorageVersion) const;

    void validateMagicBytes(common::FileInfo* fileInfo, common::offset_t& offset) const;

    void writeMagicBytes(common::FileInfo* fileInfo, common::offset_t& offset) const;

private:
    std::shared_ptr<spdlog::logger> logger;
    std::unordered_map<common::table_id_t, std::unique_ptr<NodeTableSchema>> nodeTableSchemas;
    std::unordered_map<common::table_id_t, std::unique_ptr<RelTableSchema>> relTableSchemas;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    std::unordered_map<std::string, common::table_id_t> nodeTableNameToIDMap;
    std::unordered_map<std::string, common::table_id_t> relTableNameToIDMap;
    common::table_id_t nextTableID;
};

class Catalog {
public:
    Catalog();

    explicit Catalog(storage::WAL* wal);

    virtual ~Catalog() = default;

    // TODO(Guodong): Get rid of these two functions.
    inline CatalogContent* getReadOnlyVersion() const { return catalogContentForReadOnlyTrx.get(); }
    inline CatalogContent* getWriteVersion() const { return catalogContentForWriteTrx.get(); }

    inline function::BuiltInVectorOperations* getBuiltInScalarFunctions() const {
        return builtInVectorOperations.get();
    }
    inline function::BuiltInAggregateFunctions* getBuiltInAggregateFunction() const {
        return builtInAggregateFunctions.get();
    }

    void prepareCommitOrRollback(transaction::TransactionAction action);
    void checkpointInMemory();

    inline void initCatalogContentForWriteTrxIfNecessary() {
        if (!catalogContentForWriteTrx) {
            catalogContentForWriteTrx =
                std::make_unique<CatalogContent>(*catalogContentForReadOnlyTrx);
        }
    }

    static inline void saveInitialCatalogToFile(const std::string& directory) {
        std::make_unique<Catalog>()->getReadOnlyVersion()->saveToFile(
            directory, common::DBFileType::ORIGINAL);
    }

    common::ExpressionType getFunctionType(const std::string& name) const;

    common::table_id_t addNodeTableSchema(std::string tableName, common::property_id_t primaryKeyId,
        std::vector<Property> propertyDefinitions);

    common::table_id_t addRelTableSchema(std::string tableName, RelMultiplicity relMultiplicity,
        const std::vector<Property>& propertyDefinitions, common::table_id_t srcTableID,
        common::table_id_t dstTableID);

    void dropTableSchema(common::table_id_t tableID);

    void renameTable(common::table_id_t tableID, std::string newName);

    void addProperty(
        common::table_id_t tableID, const std::string& propertyName, common::LogicalType dataType);

    void dropProperty(common::table_id_t tableID, common::property_id_t propertyID);

    void renameProperty(
        common::table_id_t tableID, common::property_id_t propertyID, const std::string& newName);

    std::unordered_set<RelTableSchema*> getAllRelTableSchemasContainBoundTable(
        common::table_id_t boundTableID) const;

private:
    inline bool hasUpdates() { return catalogContentForWriteTrx != nullptr; }

protected:
    std::unique_ptr<function::BuiltInVectorOperations> builtInVectorOperations;
    std::unique_ptr<function::BuiltInAggregateFunctions> builtInAggregateFunctions;
    std::unique_ptr<CatalogContent> catalogContentForReadOnlyTrx;
    std::unique_ptr<CatalogContent> catalogContentForWriteTrx;
    storage::WAL* wal;
};

} // namespace catalog
} // namespace kuzu
