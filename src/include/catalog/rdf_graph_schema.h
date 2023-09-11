#pragma once

#include "table_schema.h"

namespace kuzu {
namespace catalog {

class RdfGraphSchema : public TableSchema {
public:
    RdfGraphSchema(std::string tableName, common::table_id_t tableID,
        common::table_id_t nodeTableID, common::table_id_t relTableID)
        : TableSchema{std::move(tableName), tableID, common::TableType::RDF,
              std::vector<std::unique_ptr<Property>>{}},
          nodeTableID{nodeTableID}, relTableID{relTableID} {}
    RdfGraphSchema(common::table_id_t nodeTableID, common::table_id_t relTableID)
        : TableSchema{common::InternalKeyword::ANONYMOUS, common::INVALID_TABLE_ID,
              common::TableType::RDF, std::vector<std::unique_ptr<Property>>{}},
          nodeTableID{nodeTableID}, relTableID{relTableID} {}

    inline common::table_id_t getNodeTableID() const { return nodeTableID; }
    inline common::table_id_t getRelTableID() const { return relTableID; }

    static std::unique_ptr<RdfGraphSchema> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    inline std::unique_ptr<TableSchema> copy() const final {
        return std::make_unique<RdfGraphSchema>(tableName, tableID, nodeTableID, relTableID);
    }

private:
    void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) final;

private:
    common::table_id_t nodeTableID;
    common::table_id_t relTableID;
};

} // namespace catalog
} // namespace kuzu
