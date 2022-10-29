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

public:
    string name;
    DataType dataType;
};

struct Property : PropertyNameDataType {

private:
    Property(string name, DataType dataType, uint32_t propertyID, table_id_t tableID)
        : PropertyNameDataType{move(name), move(dataType)}, propertyID{propertyID}, tableID{
                                                                                        tableID} {}

public:
    // This constructor is needed for ser/deser functions
    Property() {}

    static Property constructUnstructuredNodeProperty(
        string name, uint32_t propertyID, table_id_t tableID) {
        return Property(move(name), DataType(UNSTRUCTURED), propertyID, tableID);
    }

    static Property constructStructuredNodeProperty(
        const PropertyNameDataType& nameDataType, uint32_t propertyID, table_id_t tableID) {
        return Property(nameDataType.name, nameDataType.dataType, propertyID, tableID);
    }

    static inline Property constructRelProperty(
        const PropertyNameDataType& nameDataType, uint32_t propertyID, table_id_t tableID) {
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
          primaryKeyPropertyIdx{primaryPropertyId}, structuredProperties{
                                                        move(structuredProperties)} {}

    inline uint64_t getNumStructuredProperties() const { return structuredProperties.size(); }
    void addUnstructuredProperties(vector<string>& unstructuredPropertyNames);

    inline void addFwdRelTableID(table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }

    inline Property getPrimaryKey() const { return structuredProperties[primaryKeyPropertyIdx]; }

    vector<Property> getAllNodeProperties() const;

    // TODO(Semih): When we support updating the schemas, we need to update this or, we need
    // a more robust mechanism to keep track of which property is the primary key (e.g., store this
    // information with the property). This is an idx, not an ID, so as the columns/properties of
    // the table change, the idx can change.
    uint64_t primaryKeyPropertyIdx;
    vector<Property> structuredProperties, unstructuredProperties;
    unordered_set<table_id_t> fwdRelTableIDSet; // srcNode->rel
    unordered_set<table_id_t> bwdRelTableIDSet; // dstNode->rel
    // This map is maintained as a cache for unstructured properties. It is not serialized to the
    // catalog file, but is re-constructed when reading from the catalog file.
    unordered_map<string, uint64_t> unstrPropertiesNameToIdMap;
};

struct RelTableSchema : TableSchema {
    RelTableSchema()
        : TableSchema{"", UINT64_MAX, false /* isNodeTable */}, relMultiplicity{MANY_MANY} {}
    RelTableSchema(string tableName, table_id_t tableID, RelMultiplicity relMultiplicity,
        vector<Property> properties, SrcDstTableIDs srcDstTableIDs)
        : TableSchema{move(tableName), tableID, false /* isNodeTable */},
          relMultiplicity{relMultiplicity}, properties{move(properties)}, srcDstTableIDs{move(
                                                                              srcDstTableIDs)} {}

    unordered_set<table_id_t> getAllNodeTableIDs() const;

    inline Property& getRelIDDefinition() {
        for (auto& property : properties) {
            if (property.name == INTERNAL_ID_SUFFIX) {
                return property;
            }
        }
        assert(false);
    }

    inline bool isSingleMultiplicityInDirection(RelDirection direction) const {
        return relMultiplicity == ONE_ONE ||
               relMultiplicity == (direction == FWD ? MANY_ONE : ONE_MANY);
    }

    inline SrcDstTableIDs getSrcDstTableIDs() const { return srcDstTableIDs; }

    inline uint32_t getNumProperties() const { return properties.size(); }

    inline uint32_t getNumPropertiesToReadFromCSV() const {
        // -1 here is to remove the INTERNAL_ID_SUFFIX = "_id" property.
        return properties.size() - 1;
    }
    inline bool isStoredAsLists(RelDirection relDirection) const {
        return relMultiplicity == MANY_MANY ||
               (relMultiplicity == ONE_MANY && relDirection == FWD) ||
               (relMultiplicity == MANY_ONE && relDirection == BWD);
    }

    bool edgeContainsNodeTable(table_id_t tableID) const {
        return srcDstTableIDs.srcTableIDs.contains(tableID) ||
               srcDstTableIDs.dstTableIDs.contains(tableID);
    }

    RelMultiplicity relMultiplicity;
    vector<Property> properties;
    SrcDstTableIDs srcDstTableIDs;
};
} // namespace catalog
} // namespace graphflow
