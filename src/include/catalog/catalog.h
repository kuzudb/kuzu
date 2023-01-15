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

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::storage;

namespace spdlog {
class logger;
}

namespace kuzu {
namespace catalog {
class CatalogContent {
public:
    // This constructor is only used for mock catalog testing only.
    CatalogContent();

    explicit CatalogContent(const string& directory);

    CatalogContent(const CatalogContent& other);

    virtual ~CatalogContent() = default;

    /**
     * Node and Rel table functions.
     */
    table_id_t addNodeTableSchema(string tableName, property_id_t primaryKeyId,
        vector<PropertyNameDataType> propertyDefinitions);

    table_id_t addRelTableSchema(string tableName, RelMultiplicity relMultiplicity,
        const vector<PropertyNameDataType>& propertyDefinitions,
        vector<pair<table_id_t, table_id_t>> srcDstTableIDs);

    inline bool containNodeTable(table_id_t tableID) const {
        return nodeTableSchemas.contains(tableID);
    }
    inline bool containRelTable(table_id_t tableID) const {
        return relTableSchemas.contains(tableID);
    }

    inline string getTableName(table_id_t tableID) const {
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

    inline bool containNodeTable(const string& tableName) const {
        return nodeTableNameToIDMap.contains(tableName);
    }
    inline bool containRelTable(const string& tableName) const {
        return relTableNameToIDMap.contains(tableName);
    }

    inline table_id_t getTableID(const string& tableName) const {
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
    const Property& getNodeProperty(table_id_t tableID, const string& propertyName) const;
    const Property& getRelProperty(table_id_t tableID, const string& propertyName) const;

    vector<Property> getAllNodeProperties(table_id_t tableID) const;
    inline const vector<Property>& getRelProperties(table_id_t tableID) const {
        return relTableSchemas.at(tableID)->properties;
    }
    inline vector<table_id_t> getNodeTableIDs() const {
        vector<table_id_t> nodeTableIDs;
        for (auto& [tableID, _] : nodeTableSchemas) {
            nodeTableIDs.push_back(tableID);
        }
        return nodeTableIDs;
    }
    inline vector<table_id_t> getRelTableIDs() const {
        vector<table_id_t> relTableIDs;
        for (auto& [tableID, _] : relTableSchemas) {
            relTableIDs.push_back(tableID);
        }
        return relTableIDs;
    }
    inline unordered_map<table_id_t, unique_ptr<NodeTableSchema>>& getNodeTableSchemas() {
        return nodeTableSchemas;
    }
    inline unordered_map<table_id_t, unique_ptr<RelTableSchema>>& getRelTableSchemas() {
        return relTableSchemas;
    }
    inline unordered_set<table_id_t> getNodeTableIDsForRelTableDirection(
        table_id_t relTableID, RelDirection direction) const {
        auto relTableSchema = getRelTableSchema(relTableID);
        return direction == FWD ? relTableSchema->getUniqueSrcTableIDs() :
                                  relTableSchema->getUniqueDstTableIDs();
    }

    void dropTableSchema(table_id_t tableID);

    void saveToFile(const string& directory, DBFileType dbFileType);
    void readFromFile(const string& directory, DBFileType dbFileType);

private:
    inline table_id_t assignNextTableID() { return nextTableID++; }

private:
    shared_ptr<spdlog::logger> logger;
    unordered_map<table_id_t, unique_ptr<NodeTableSchema>> nodeTableSchemas;
    unordered_map<table_id_t, unique_ptr<RelTableSchema>> relTableSchemas;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    unordered_map<string, table_id_t> nodeTableNameToIDMap;
    unordered_map<string, table_id_t> relTableNameToIDMap;
    table_id_t nextTableID;
};

class Catalog {
public:
    Catalog();

    explicit Catalog(WAL* wal);

    virtual ~Catalog() = default;

    inline CatalogContent* getReadOnlyVersion() const { return catalogContentForReadOnlyTrx.get(); }

    inline CatalogContent* getWriteVersion() const { return catalogContentForWriteTrx.get(); }

    inline BuiltInVectorOperations* getBuiltInScalarFunctions() const {
        return builtInVectorOperations.get();
    }
    inline BuiltInAggregateFunctions* getBuiltInAggregateFunction() const {
        return builtInAggregateFunctions.get();
    }

    inline bool hasUpdates() { return catalogContentForWriteTrx != nullptr; }

    void checkpointInMemoryIfNecessary();

    inline void initCatalogContentForWriteTrxIfNecessary() {
        if (!catalogContentForWriteTrx) {
            catalogContentForWriteTrx = make_unique<CatalogContent>(*catalogContentForReadOnlyTrx);
        }
    }

    inline void writeCatalogForWALRecord(const string& directory) {
        catalogContentForWriteTrx->saveToFile(directory, DBFileType::WAL_VERSION);
    }

    static inline void saveInitialCatalogToFile(const string& directory) {
        make_unique<Catalog>()->getReadOnlyVersion()->saveToFile(directory, DBFileType::ORIGINAL);
    }

    ExpressionType getFunctionType(const string& name) const;

    table_id_t addNodeTableSchema(string tableName, property_id_t primaryKeyId,
        vector<PropertyNameDataType> propertyDefinitions);

    table_id_t addRelTableSchema(string tableName, RelMultiplicity relMultiplicity,
        const vector<PropertyNameDataType>& propertyDefinitions,
        vector<pair<table_id_t, table_id_t>> srcDstTableIDs);

    void dropTableSchema(table_id_t tableID);

    void dropProperty(table_id_t tableID, property_id_t propertyID);

    void addProperty(table_id_t tableID, string propertyName, DataType dataType);

protected:
    unique_ptr<BuiltInVectorOperations> builtInVectorOperations;
    unique_ptr<BuiltInAggregateFunctions> builtInAggregateFunctions;
    unique_ptr<CatalogContent> catalogContentForReadOnlyTrx;
    unique_ptr<CatalogContent> catalogContentForWriteTrx;
    WAL* wal;
};

} // namespace catalog
} // namespace kuzu
