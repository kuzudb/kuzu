#pragma once

#include <memory>
#include <utility>

#include "catalog_structs.h"
#include "nlohmann/json.hpp"

#include "src/common/include/assert.h"
#include "src/common/include/exception.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/ser_deser.h"
#include "src/common/include/utils.h"
#include "src/function/aggregate/include/built_in_aggregate_functions.h"
#include "src/function/include/built_in_vector_operations.h"
#include "src/storage/wal/include/wal.h"

using namespace graphflow::common;
using namespace graphflow::function;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace catalog {

typedef vector<atomic<uint64_t>> atomic_uint64_vec_t;

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
    table_id_t addNodeTableSchema(string tableName, uint32_t primaryKeyIdx,
        vector<PropertyNameDataType> predefinedPropertyNameDataTypes);

    table_id_t addRelTableSchema(string tableName, RelMultiplicity relMultiplicity,
        vector<PropertyNameDataType> propertyNameDataTypes, SrcDstTableIDs srcDstTableIDs);

    virtual inline string getNodeTableName(table_id_t tableID) const {
        return nodeTableSchemas.at(tableID)->tableName;
    }
    virtual inline string getRelTableName(table_id_t tableID) const {
        return relTableSchemas.at(tableID)->tableName;
    }

    inline NodeTableSchema* getNodeTableSchema(table_id_t tableID) const {
        return nodeTableSchemas.at(tableID).get();
    }
    inline RelTableSchema* getRelTableSchema(table_id_t tableID) const {
        return relTableSchemas.at(tableID).get();
    }

    inline uint64_t getNumNodeTables() const { return nodeTableSchemas.size(); }

    virtual inline bool containNodeTable(const string& tableName) const {
        return end(nodeTableNameToIDMap) != nodeTableNameToIDMap.find(tableName);
    }
    virtual inline bool containRelTable(const string& tableName) const {
        return end(relTableNameToIDMap) != relTableNameToIDMap.find(tableName);
    }

    virtual inline table_id_t getNodeTableIDFromName(const string& tableName) const {
        return nodeTableNameToIDMap.at(tableName);
    }
    virtual inline table_id_t getRelTableIDFromName(const string& tableName) const {
        return relTableNameToIDMap.at(tableName);
    }

    virtual inline bool isSingleMultiplicityInDirection(
        table_id_t tableID, RelDirection direction) const {
        return relTableSchemas.at(tableID)->isSingleMultiplicityInDirection(direction);
    }

    /**
     * Node and Rel property functions.
     */
    virtual bool containNodeProperty(table_id_t tableID, const string& propertyName) const;
    virtual bool containRelProperty(table_id_t tableID, const string& propertyName) const;

    // getNodeProperty and getRelProperty should be called after checking if property exists
    // (containNodeProperty and containRelProperty).
    virtual const Property& getNodeProperty(table_id_t tableID, const string& propertyName) const;
    virtual const Property& getRelProperty(table_id_t tableID, const string& propertyName) const;

    const Property& getNodePrimaryKeyProperty(table_id_t tableID) const;

    vector<Property> getAllNodeProperties(table_id_t tableID) const;
    inline const vector<Property>& getPredefinedNodeProperties(table_id_t tableID) const {
        return nodeTableSchemas.at(tableID)->predefinedProperties;
    }
    inline const vector<Property>& getAdhocNodeProperties(table_id_t tableID) const {
        return nodeTableSchemas.at(tableID)->adhocProperties;
    }
    inline const vector<Property>& getRelProperties(table_id_t tableID) const {
        return relTableSchemas.at(tableID)->properties;
    }
    inline const unordered_map<string, uint64_t>& getUnstrPropertiesNameToIdMap(
        table_id_t tableID) const {
        return nodeTableSchemas.at(tableID)->adhocPropertiesNameToIdMap;
    }
    inline unordered_map<table_id_t, unique_ptr<NodeTableSchema>>& getNodeTableSchemas() {
        return nodeTableSchemas;
    }
    inline unordered_map<table_id_t, unique_ptr<RelTableSchema>>& getRelTableSchemas() {
        return relTableSchemas;
    }

    void removeTableSchema(TableSchema* tableSchema);

    /**
     * Graph topology functions.
     */

    const unordered_set<table_id_t>& getNodeTableIDsForRelTableDirection(
        table_id_t tableID, RelDirection direction) const;
    virtual const unordered_set<table_id_t>& getRelTableIDsForNodeTableDirection(
        table_id_t tableID, RelDirection direction) const;

    void saveToFile(const string& directory, DBFileType dbFileType);
    void readFromFile(const string& directory, DBFileType dbFileType);

private:
    shared_ptr<spdlog::logger> logger;
    unordered_map<table_id_t, unique_ptr<NodeTableSchema>> nodeTableSchemas;
    unordered_map<table_id_t, unique_ptr<RelTableSchema>> relTableSchemas;
    // These two maps are maintained as caches. They are not serialized to the catalog file, but
    // is re-constructed when reading from the catalog file.
    unordered_map<string, table_id_t> nodeTableNameToIDMap;
    unordered_map<string, table_id_t> relTableNameToIDMap;
};

class Catalog {
public:
    // This constructor is only used for mock catalog testing only.
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

    inline void writeCatalogForWALRecord(string directory) {
        catalogContentForWriteTrx->saveToFile(move(directory), DBFileType::WAL_VERSION);
    }

    static inline void saveInitialCatalogToFile(const string& directory) {
        make_unique<Catalog>()->getReadOnlyVersion()->saveToFile(directory, DBFileType::ORIGINAL);
    }

    ExpressionType getFunctionType(const string& name) const;

    table_id_t addNodeTableSchema(string tableName, uint32_t primaryKeyIdx,
        vector<PropertyNameDataType> predefinedPropertyNameDataTypes);

    table_id_t addRelTableSchema(string tableName, RelMultiplicity relMultiplicity,
        vector<PropertyNameDataType> propertyNameDataTypes, SrcDstTableIDs srcDstTableIDs);

    inline void setAdhocPropertiesOfNodeTableSchema(
        vector<PropertyNameDataType>& adhocProperties, table_id_t tableID) {
        initCatalogContentForWriteTrxIfNecessary();
        catalogContentForWriteTrx->getNodeTableSchema(tableID)->addAdhocProperties(adhocProperties);
    }

    inline void removeTableSchema(TableSchema* tableSchema) {
        initCatalogContentForWriteTrxIfNecessary();
        catalogContentForWriteTrx->removeTableSchema(tableSchema);
        wal->logDropTableRecord(tableSchema->isNodeTable, tableSchema->tableID);
    }

protected:
    unique_ptr<BuiltInVectorOperations> builtInVectorOperations;
    unique_ptr<BuiltInAggregateFunctions> builtInAggregateFunctions;
    unique_ptr<CatalogContent> catalogContentForReadOnlyTrx;
    unique_ptr<CatalogContent> catalogContentForWriteTrx;
    WAL* wal;
};

} // namespace catalog
} // namespace graphflow
