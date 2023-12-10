#pragma once

#include "table_schema.h"

namespace kuzu {
namespace catalog {

class RelTableGroupSchema : public TableSchema {
public:
    RelTableGroupSchema() : TableSchema{common::TableType::REL_GROUP} {}
    RelTableGroupSchema(std::string tableName, common::table_id_t tableID,
        std::vector<common::table_id_t> relTableIDs)
        : TableSchema{std::move(tableName), tableID, common::TableType::REL_GROUP,
              property_vector_t{}},
          relTableIDs{std::move(relTableIDs)} {}
    RelTableGroupSchema(const RelTableGroupSchema& other);

    inline std::vector<common::table_id_t> getRelTableIDs() const { return relTableIDs; }

    static std::unique_ptr<RelTableGroupSchema> deserialize(common::Deserializer& deserializer);

    inline std::unique_ptr<TableSchema> copy() const final {
        return std::make_unique<RelTableGroupSchema>(tableName, tableID, relTableIDs);
    }

private:
    void serializeInternal(common::Serializer& serializer) override;

private:
    std::vector<common::table_id_t> relTableIDs;
};

} // namespace catalog
} // namespace kuzu
