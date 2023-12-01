#pragma once

#include "common/constants.h"
#include "common/enums/rel_direction.h"
#include "table_schema.h"

namespace kuzu {
namespace catalog {

enum class RelMultiplicity : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };
RelMultiplicity getRelMultiplicityFromString(const std::string& relMultiplicityString);
std::string getRelMultiplicityAsString(RelMultiplicity relMultiplicity);

class RelTableSchema : public TableSchema {
public:
    static constexpr uint64_t INTERNAL_REL_ID_PROPERTY_ID = 0;

    RelTableSchema(RelMultiplicity relMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID)
        : TableSchema{common::InternalKeyword::ANONYMOUS, common::INVALID_TABLE_ID,
              common::TableType::REL, {} /* properties */},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}
    RelTableSchema(std::string tableName, common::table_id_t tableID,
        RelMultiplicity relMultiplicity, std::vector<std::unique_ptr<Property>> properties,
        common::table_id_t srcTableID, common::table_id_t dstTableID)
        : TableSchema{std::move(tableName), tableID, common::TableType::REL, std::move(properties)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}
    RelTableSchema(std::string tableName, common::table_id_t tableID,
        std::vector<std::unique_ptr<Property>> properties, std::string comment,
        common::property_id_t nextPropertyID, RelMultiplicity relMultiplicity,
        common::table_id_t srcTableID, common::table_id_t dstTableID)
        : TableSchema{common::TableType::REL, std::move(tableName), tableID, std::move(properties),
              std::move(comment), nextPropertyID},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}

    inline bool isSingleMultiplicityInDirection(common::RelDataDirection direction) const {
        return relMultiplicity == RelMultiplicity::ONE_ONE ||
               relMultiplicity == (direction == common::RelDataDirection::FWD ?
                                          RelMultiplicity::MANY_ONE :
                                          RelMultiplicity::ONE_MANY);
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
    inline common::table_id_t getSrcTableID() const { return srcTableID; }
    inline common::table_id_t getDstTableID() const { return dstTableID; }

    static std::unique_ptr<RelTableSchema> deserialize(common::Deserializer& deserializer);

    inline std::unique_ptr<TableSchema> copy() const override {
        return std::make_unique<RelTableSchema>(tableName, tableID, Property::copy(properties),
            comment, nextPropertyID, relMultiplicity, srcTableID, dstTableID);
    }

private:
    void serializeInternal(common::Serializer& serializer) final;

private:
    RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
};

} // namespace catalog
} // namespace kuzu
