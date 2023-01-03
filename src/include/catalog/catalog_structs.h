#pragma once

#include <unordered_map>
#include <unordered_set>

#include "common/configs.h"
#include "common/exception.h"
#include "common/types/types_include.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

enum RelMultiplicity : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };
RelMultiplicity getRelMultiplicityFromString(const string& relMultiplicityString);
string getRelMultiplicityAsString(RelMultiplicity relMultiplicity);

// A PropertyNameDataType consists of its name, id, and dataType.
struct PropertyNameDataType {
    // This constructor is needed for ser/deser functions
    PropertyNameDataType(){};
    PropertyNameDataType(string name, DataTypeID dataTypeID)
        : PropertyNameDataType{std::move(name), DataType(dataTypeID)} {
        assert(dataTypeID != LIST);
    }
    PropertyNameDataType(string name, DataType dataType)
        : name{std::move(name)}, dataType{std::move(dataType)} {};
    virtual ~PropertyNameDataType() = default;

    string name;
    DataType dataType;
};

struct Property : PropertyNameDataType {
private:
    Property(string name, DataType dataType, uint32_t propertyID, table_id_t tableID)
        : PropertyNameDataType{std::move(name), std::move(dataType)},
          propertyID{propertyID}, tableID{tableID} {}

public:
    // This constructor is needed for ser/deser functions
    Property() {}

    static Property constructNodeProperty(
        const PropertyNameDataType& nameDataType, uint32_t propertyID, table_id_t tableID) {
        return Property(nameDataType.name, nameDataType.dataType, propertyID, tableID);
    }

    static inline Property constructRelProperty(
        const PropertyNameDataType& nameDataType, uint32_t propertyID, table_id_t tableID) {
        return Property(nameDataType.name, nameDataType.dataType, propertyID, tableID);
    }

    property_id_t propertyID;
    table_id_t tableID;
};

struct TableSchema {
public:
    TableSchema(string tableName, table_id_t tableID, bool isNodeTable)
        : tableName{std::move(tableName)}, tableID{tableID}, isNodeTable{isNodeTable} {}

    virtual ~TableSchema() = default;

    static inline bool isReservedPropertyName(const string& propertyName) {
        return propertyName == INTERNAL_ID_SUFFIX;
    }

public:
    string tableName;
    table_id_t tableID;
    bool isNodeTable;
};

struct NodeTableSchema : TableSchema {
    NodeTableSchema()
        : NodeTableSchema{"", INVALID_TABLE_ID, INVALID_PROPERTY_ID, vector<Property>{}} {}
    NodeTableSchema(string tableName, table_id_t tableID, property_id_t primaryPropertyId,
        vector<Property> properties)
        : TableSchema{std::move(tableName), tableID, true /* isNodeTable */},
          primaryKeyPropertyIdx{primaryPropertyId}, properties{std::move(properties)} {}

    inline property_id_t getNumProperties() const { return properties.size(); }

    inline void addFwdRelTableID(table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }

    inline Property getPrimaryKey() const { return properties[primaryKeyPropertyIdx]; }

    inline vector<Property> getAllNodeProperties() const { return properties; }

    // TODO(Semih): When we support updating the schemas, we need to update this or, we need
    // a more robust mechanism to keep track of which property is the primary key (e.g., store this
    // information with the property). This is an idx, not an ID, so as the columns/properties of
    // the table change, the idx can change.
    property_id_t primaryKeyPropertyIdx;
    vector<Property> properties;
    unordered_set<table_id_t> fwdRelTableIDSet; // srcNode->rel
    unordered_set<table_id_t> bwdRelTableIDSet; // dstNode->rel
};

struct RelTableSchema : TableSchema {
public:
    static constexpr uint64_t INTERNAL_REL_ID_PROPERTY_IDX = 0;

    RelTableSchema()
        : TableSchema{"", INVALID_TABLE_ID, false /* isNodeTable */}, relMultiplicity{MANY_MANY} {}
    RelTableSchema(string tableName, table_id_t tableID, RelMultiplicity relMultiplicity,
        vector<Property> properties, vector<pair<table_id_t, table_id_t>> srcDstTableIDs)
        : TableSchema{std::move(tableName), tableID, false /* isNodeTable */},
          relMultiplicity{relMultiplicity}, properties{std::move(properties)},
          srcDstTableIDs{std::move(srcDstTableIDs)} {}

    inline Property& getRelIDDefinition() {
        for (auto& property : properties) {
            if (property.name == INTERNAL_ID_SUFFIX) {
                return property;
            }
        }
        throw InternalException("Cannot find internal rel ID definition.");
    }

    inline bool isSingleMultiplicityInDirection(RelDirection direction) const {
        return relMultiplicity == ONE_ONE ||
               relMultiplicity == (direction == FWD ? MANY_ONE : ONE_MANY);
    }

    inline vector<pair<table_id_t, table_id_t>> getSrcDstTableIDs() const { return srcDstTableIDs; }

    inline uint32_t getNumProperties() const { return properties.size(); }

    inline uint32_t getNumUserDefinedProperties() const {
        // Note: the first column stores the relID property.
        return properties.size() - 1;
    }
    inline bool isStoredAsLists(RelDirection relDirection) const {
        return relMultiplicity == MANY_MANY ||
               (relMultiplicity == ONE_MANY && relDirection == FWD) ||
               (relMultiplicity == MANY_ONE && relDirection == BWD);
    }

    inline bool edgeContainsNodeTable(table_id_t tableID) const {
        return any_of(srcDstTableIDs.begin(), srcDstTableIDs.end(),
            [tableID](pair<table_id_t, table_id_t> srcDstTableID) {
                return srcDstTableID.first == tableID || srcDstTableID.second == tableID;
            });
    }

    unordered_set<table_id_t> getAllNodeTableIDs() const;

    unordered_set<table_id_t> getUniqueSrcTableIDs() const;

    unordered_set<table_id_t> getUniqueDstTableIDs() const;

    unordered_set<table_id_t> getUniqueNbrTableIDsForBoundTableIDDirection(
        RelDirection direction, table_id_t boundTableID) const;

    RelMultiplicity relMultiplicity;
    vector<Property> properties;
    vector<pair<table_id_t, table_id_t>> srcDstTableIDs;
};

} // namespace catalog
} // namespace kuzu
