#pragma once

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

#include "common/constants.h"
#include "common/exception.h"
#include "common/rel_direction.h"
#include "common/types/types_include.h"

namespace kuzu {
namespace catalog {

enum RelMultiplicity : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };
RelMultiplicity getRelMultiplicityFromString(const std::string& relMultiplicityString);
std::string getRelMultiplicityAsString(RelMultiplicity relMultiplicity);

struct Property {
public:
    static constexpr std::string_view REL_FROM_PROPERTY_NAME = "_FROM_";
    static constexpr std::string_view REL_TO_PROPERTY_NAME = "_TO_";

    // This constructor is needed for ser/deser functions
    Property() : Property{"", common::LogicalType{}} {};
    Property(std::string name, common::LogicalType dataType)
        : Property{std::move(name), std::move(dataType), common::INVALID_PROPERTY_ID,
              common::INVALID_TABLE_ID} {}
    Property(std::string name, common::LogicalType dataType, common::property_id_t propertyID,
        common::table_id_t tableID)
        : name{std::move(name)}, dataType{std::move(dataType)},
          propertyID{propertyID}, tableID{tableID} {}

    std::string name;
    common::LogicalType dataType;
    common::property_id_t propertyID;
    common::table_id_t tableID;
};

struct TableSchema {
public:
    TableSchema(std::string tableName, common::table_id_t tableID, bool isNodeTable,
        std::vector<Property> properties)
        : tableName{std::move(tableName)}, tableID{tableID}, isNodeTable{isNodeTable},
          properties{std::move(properties)}, nextPropertyID{
                                                 (common::property_id_t)this->properties.size()} {}

    virtual ~TableSchema() = default;

    static inline bool isReservedPropertyName(const std::string& propertyName) {
        return propertyName == common::InternalKeyword::ID;
    }

    inline uint32_t getNumProperties() const { return properties.size(); }

    inline void dropProperty(common::property_id_t propertyID) {
        properties.erase(std::remove_if(properties.begin(), properties.end(),
                             [propertyID](const Property& property) {
                                 return property.propertyID == propertyID;
                             }),
            properties.end());
    }

    inline bool containProperty(std::string propertyName) const {
        return std::any_of(properties.begin(), properties.end(),
            [&propertyName](const Property& property) { return property.name == propertyName; });
    }

    inline void addProperty(std::string propertyName, common::LogicalType dataType) {
        properties.emplace_back(
            std::move(propertyName), std::move(dataType), increaseNextPropertyID(), tableID);
    }

    std::string getPropertyName(common::property_id_t propertyID) const;

    common::property_id_t getPropertyID(const std::string& propertyName) const;

    Property getProperty(common::property_id_t propertyID) const;

    void renameProperty(common::property_id_t propertyID, const std::string& newName);

private:
    inline common::property_id_t increaseNextPropertyID() { return nextPropertyID++; }

public:
    std::string tableName;
    common::table_id_t tableID;
    bool isNodeTable;
    std::vector<Property> properties;
    common::property_id_t nextPropertyID;
};

struct NodeTableSchema : TableSchema {
    NodeTableSchema()
        : NodeTableSchema{
              "", common::INVALID_TABLE_ID, common::INVALID_PROPERTY_ID, std::vector<Property>{}} {}
    NodeTableSchema(std::string tableName, common::table_id_t tableID,
        common::property_id_t primaryPropertyId, std::vector<Property> properties)
        : TableSchema{std::move(tableName), tableID, true /* isNodeTable */, std::move(properties)},
          primaryKeyPropertyID{primaryPropertyId} {}

    inline void addFwdRelTableID(common::table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(common::table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }

    inline Property getPrimaryKey() const { return properties[primaryKeyPropertyID]; }

    inline std::vector<Property> getAllNodeProperties() const { return properties; }

    // TODO(Semih): When we support updating the schemas, we need to update this or, we need
    // a more robust mechanism to keep track of which property is the primary key (e.g., store this
    // information with the property). This is an idx, not an ID, so as the columns/properties of
    // the table change, the idx can change.
    common::property_id_t primaryKeyPropertyID;
    std::unordered_set<common::table_id_t> fwdRelTableIDSet; // srcNode->rel
    std::unordered_set<common::table_id_t> bwdRelTableIDSet; // dstNode->rel
};

struct RelTableSchema : TableSchema {
public:
    static constexpr uint64_t INTERNAL_REL_ID_PROPERTY_ID = 0;

    RelTableSchema()
        : TableSchema{"", common::INVALID_TABLE_ID, false /* isNodeTable */, {} /* properties */},
          relMultiplicity{MANY_MANY}, srcTableID{common::INVALID_TABLE_ID},
          dstTableID{common::INVALID_TABLE_ID} {}
    RelTableSchema(std::string tableName, common::table_id_t tableID,
        RelMultiplicity relMultiplicity, std::vector<Property> properties,
        common::table_id_t srcTableID, common::table_id_t dstTableID)
        : TableSchema{std::move(tableName), tableID, false /* isNodeTable */,
              std::move(properties)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}

    inline Property& getRelIDDefinition() {
        for (auto& property : properties) {
            if (property.name == common::InternalKeyword::ID) {
                return property;
            }
        }
        throw common::InternalException("Cannot find internal rel ID definition.");
    }

    inline bool isSingleMultiplicityInDirection(common::RelDataDirection direction) const {
        return relMultiplicity == ONE_ONE ||
               relMultiplicity ==
                   (direction == common::RelDataDirection::FWD ? MANY_ONE : ONE_MANY);
    }

    inline uint32_t getNumUserDefinedProperties() const {
        // Note: the first column stores the relID property.
        return properties.size() - 1;
    }

    inline bool isSrcOrDstTable(common::table_id_t tableID) const {
        return srcTableID == tableID || dstTableID == tableID;
    }

    inline common::table_id_t getBoundTableID(common::RelDataDirection relDirection) const {
        return relDirection == common::RelDataDirection::FWD ? srcTableID : dstTableID;
    }

    inline common::table_id_t getNbrTableID(common::RelDataDirection relDirection) const {
        return relDirection == common::RelDataDirection::FWD ? dstTableID : srcTableID;
    }

    RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
};

} // namespace catalog
} // namespace kuzu
