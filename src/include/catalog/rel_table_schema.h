#pragma once

#include "common/rel_direction.h"
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
        common::table_id_t dstTableID, std::unique_ptr<common::LogicalType> srcPKDataType,
        std::unique_ptr<common::LogicalType> dstPKDataType)
        : TableSchema{common::InternalKeyword::ANONYMOUS, common::INVALID_TABLE_ID,
              common::TableType::REL, {} /* properties */},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPKDataType{std::move(srcPKDataType)}, dstPKDataType{std::move(dstPKDataType)} {}
    RelTableSchema(std::string tableName, common::table_id_t tableID,
        RelMultiplicity relMultiplicity, std::vector<std::unique_ptr<Property>> properties,
        common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::unique_ptr<common::LogicalType> srcPKDataType,
        std::unique_ptr<common::LogicalType> dstPKDataType)
        : TableSchema{std::move(tableName), tableID, common::TableType::REL, std::move(properties)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPKDataType{std::move(srcPKDataType)}, dstPKDataType{std::move(dstPKDataType)} {}
    RelTableSchema(std::string tableName, common::table_id_t tableID,
        std::vector<std::unique_ptr<Property>> properties, std::string comment,
        common::property_id_t nextPropertyID, RelMultiplicity relMultiplicity,
        common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::unique_ptr<common::LogicalType> srcPKDataType,
        std::unique_ptr<common::LogicalType> dstPKDataType)
        : TableSchema{common::TableType::REL, std::move(tableName), tableID, std::move(properties),
              std::move(comment), nextPropertyID},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPKDataType{std::move(srcPKDataType)}, dstPKDataType{std::move(dstPKDataType)} {}

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

    inline RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline common::table_id_t getSrcTableID() const { return srcTableID; }

    inline common::table_id_t getDstTableID() const { return dstTableID; }

    inline common::LogicalType* getSrcPKDataType() const { return srcPKDataType.get(); }

    inline common::LogicalType* getDstPKDataType() const { return dstPKDataType.get(); }

    static std::unique_ptr<RelTableSchema> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    inline std::unique_ptr<TableSchema> copy() const override {
        return std::make_unique<RelTableSchema>(tableName, tableID, Property::copy(properties),
            comment, nextPropertyID, relMultiplicity, srcTableID, dstTableID, srcPKDataType->copy(),
            dstPKDataType->copy());
    }

private:
    void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) final;

private:
    RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    std::unique_ptr<common::LogicalType> srcPKDataType;
    std::unique_ptr<common::LogicalType> dstPKDataType;
};

} // namespace catalog
} // namespace kuzu
