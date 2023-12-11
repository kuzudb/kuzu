#pragma once

#include "common/enums/rel_direction.h"
#include "table_schema.h"

namespace kuzu {
namespace catalog {

enum class RelMultiplicity : uint8_t { MANY, ONE };
struct RelMultiplicityUtils {
    static RelMultiplicity getFwd(const std::string& multiplicityStr);
    static RelMultiplicity getBwd(const std::string& multiplicityStr);
};

class RelTableSchema : public TableSchema {
public:
    RelTableSchema() : TableSchema(common::TableType::REL) {}
    RelTableSchema(std::string tableName, common::table_id_t tableID, property_vector_t properties,
        RelMultiplicity srcMultiplicity, RelMultiplicity dstMultiplicity,
        common::table_id_t srcTableID, common::table_id_t dstTableID)
        : TableSchema{std::move(tableName), tableID, common::TableType::REL, std::move(properties)},
          srcMultiplicity{srcMultiplicity}, dstMultiplicity{dstMultiplicity},
          srcTableID{srcTableID}, dstTableID{dstTableID} {}
    RelTableSchema(const RelTableSchema& other);

    inline bool isSingleMultiplicity(common::RelDataDirection direction) const {
        return getMultiplicity(direction) == RelMultiplicity::ONE;
    }
    inline RelMultiplicity getMultiplicity(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? dstMultiplicity : srcMultiplicity;
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
        return std::make_unique<RelTableSchema>(*this);
    }

private:
    void serializeInternal(common::Serializer& serializer) final;

private:
    RelMultiplicity srcMultiplicity;
    RelMultiplicity dstMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
};

} // namespace catalog
} // namespace kuzu
