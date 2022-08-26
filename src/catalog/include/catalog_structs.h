#pragma once

#include <unordered_map>
#include <unordered_set>

#include "src/common/include/configs.h"
#include "src/common/types/include/types_include.h"

using namespace graphflow::common;

namespace graphflow {
namespace catalog {

struct SrcDstTableIDs {
    SrcDstTableIDs(unordered_set<table_id_t> srcTableIDs, unordered_set<table_id_t> dstTableIDs)
        : srcTableIDs{move(srcTableIDs)}, dstTableIDs{move(dstTableIDs)} {}
    SrcDstTableIDs() = default;
    unordered_set<table_id_t> srcTableIDs;
    unordered_set<table_id_t> dstTableIDs;
};

enum RelMultiplicity : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };
RelMultiplicity getRelMultiplicityFromString(const string& relMultiplicityString);

// A PropertyNameDataType consists of its name, id, and dataType. If the property is unstructured,
// then the dataType's typeID is UNSTRUCTURED, otherwise it is one of those supported by the system.
struct PropertyNameDataType {

public:
    // This constructor is needed for ser/deser functions
    PropertyNameDataType(){};

    PropertyNameDataType(string name, DataTypeID dataTypeID)
        : PropertyNameDataType{move(name), DataType(dataTypeID)} {
        assert(dataTypeID != LIST);
    }

    PropertyNameDataType(string name, DataType dataType)
        : name{move(name)}, dataType{move(dataType)} {};

    inline bool isIDProperty() const { return name == CopyCSVConfig::ID_FIELD; }

public:
    string name;
    DataType dataType;
};

struct Property : PropertyNameDataType {

private:
    Property(string name, DataType dataType, uint32_t propertyID, table_id_t tableID)
        : PropertyNameDataType{name, dataType}, propertyID{propertyID}, tableID{tableID} {}

public:
    // This constructor is needed for ser/deser functions
    Property() {}

    static Property constructUnstructuredNodeProperty(
        string name, uint32_t propertyID, table_id_t tableID) {
        return Property(name, DataType(UNSTRUCTURED), propertyID, tableID);
    }

    static Property constructStructuredNodeProperty(
        PropertyNameDataType nameDataType, uint32_t propertyID, table_id_t tableID) {
        return Property(nameDataType.name, nameDataType.dataType, propertyID, tableID);
    }

    static inline Property constructRelProperty(
        PropertyNameDataType nameDataType, uint32_t propertyID, table_id_t tableID) {
        return Property(nameDataType.name, nameDataType.dataType, propertyID, tableID);
    }

public:
    uint32_t propertyID;
    table_id_t tableID;
};

struct TableSchema {
public:
    TableSchema(string tableName, table_id_t tableID, bool isNodeTable)
        : tableName{move(tableName)}, tableID{tableID}, isNodeTable{isNodeTable} {}

public:
    string tableName;
    table_id_t tableID;
    bool isNodeTable;
};

struct NodeTableSchema : TableSchema {
    NodeTableSchema() : NodeTableSchema{"", UINT64_MAX, UINT64_MAX, vector<Property>{}} {}
    NodeTableSchema(string tableName, table_id_t tableID, uint64_t primaryPropertyId,
        vector<Property> structuredProperties)
        : TableSchema{move(tableName), tableID, true /* isNodeTable */},
          primaryPropertyId{primaryPropertyId}, structuredProperties{move(structuredProperties)} {}

    inline uint64_t getNumStructuredProperties() const { return structuredProperties.size(); }
    void addUnstructuredProperties(vector<string>& unstructuredPropertyNames);

    inline void addFwdRelTableID(table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }

    inline Property getPrimaryKey() const { return structuredProperties[primaryPropertyId]; }

    vector<Property> getAllNodeProperties() const;

    uint64_t primaryPropertyId;
    vector<Property> structuredProperties, unstructuredProperties;
    unordered_set<table_id_t> fwdRelTableIDSet; // srcNode->rel
    unordered_set<table_id_t> bwdRelTableIDSet; // dstNode->rel
    // This map is maintained as a cache for unstructured properties. It is not serialized to the
    // catalog file, but is re-constructed when reading from the catalog file.
    unordered_map<string, uint64_t> unstrPropertiesNameToIdMap;
};

struct RelTableSchema : TableSchema {
    RelTableSchema()
        : TableSchema{"", UINT64_MAX, false /* isNodeTable */}, relMultiplicity{MANY_MANY},
          numGeneratedProperties{UINT32_MAX}, numRels{0} {}
    RelTableSchema(string tableName, table_id_t tableID, RelMultiplicity relMultiplicity,
        vector<Property> properties, SrcDstTableIDs srcDstTableIDs);

    unordered_set<table_id_t> getAllNodeTableIDs() const;

    inline Property& getRelIDDefinition() {
        for (auto& property : properties) {
            if (property.name == INTERNAL_ID_SUFFIX) {
                return property;
            }
        }
        assert(false);
    }

    inline uint32_t getNumProperties() const { return properties.size(); }
    inline uint32_t getNumPropertiesToReadFromCSV() const {
        assert(properties.size() >= numGeneratedProperties);
        return properties.size() - numGeneratedProperties;
    }
    inline uint64_t getNumRels() const { return numRels; }
    inline void setNumRels(uint64_t newNumRels) { numRels = newNumRels; }

    RelMultiplicity relMultiplicity;
    uint32_t numGeneratedProperties;
    vector<Property> properties;
    SrcDstTableIDs srcDstTableIDs;
    // Cardinality information
    vector<unordered_map<table_id_t, uint64_t>> numRelsPerDirectionBoundTableID;
    uint64_t numRels;
};
} // namespace catalog
} // namespace graphflow
