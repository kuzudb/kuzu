#pragma once

#include "common/constants.h"
#include "table_schema.h"

namespace kuzu {
namespace catalog {

class RdfGraphSchema : public TableSchema {
public:
    RdfGraphSchema()
        : TableSchema{common::InternalKeyword::ANONYMOUS, common::INVALID_TABLE_ID,
              common::TableType::RDF, std::vector<std::unique_ptr<Property>>{}} {}
    RdfGraphSchema(std::string tableName, common::table_id_t rdfID,
        common::table_id_t resourceTableID, common::table_id_t literalTabelID,
        common::table_id_t resourceTripleTableID, common::table_id_t literalTripleTableID)
        : TableSchema{std::move(tableName), rdfID, common::TableType::RDF,
              std::vector<std::unique_ptr<Property>>{}},
          resourceTableID{resourceTableID}, literalTableID{literalTabelID},
          resourceTripleTableID{resourceTripleTableID}, literalTripleTableID{literalTripleTableID} {
    }

    inline common::table_id_t getResourceTableID() const { return resourceTableID; }
    inline common::table_id_t getLiteralTableID() const { return literalTableID; }
    inline common::table_id_t getResourceTripleTableID() const { return resourceTripleTableID; }
    inline common::table_id_t getLiteralTripleTableID() const { return literalTripleTableID; }

    static std::unique_ptr<RdfGraphSchema> deserialize(common::Deserializer& deserializer);

    inline std::unique_ptr<TableSchema> copy() const final {
        return std::make_unique<RdfGraphSchema>(tableName, tableID, resourceTableID, literalTableID,
            resourceTripleTableID, literalTripleTableID);
    }

private:
    void serializeInternal(common::Serializer& serializer) final;

private:
    common::table_id_t resourceTableID;
    common::table_id_t literalTableID;
    common::table_id_t resourceTripleTableID;
    common::table_id_t literalTripleTableID;
};

} // namespace catalog
} // namespace kuzu
