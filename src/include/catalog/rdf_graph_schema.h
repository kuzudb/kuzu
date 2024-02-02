#pragma once

#include "table_schema.h"

namespace kuzu {
namespace catalog {

class RdfGraphSchema : public TableSchema {
public:
    RdfGraphSchema() : TableSchema{common::TableType::RDF} {}
    RdfGraphSchema(std::string tableName, common::table_id_t rdfID,
        common::table_id_t resourceTableID, common::table_id_t literalTabelID,
        common::table_id_t resourceTripleTableID, common::table_id_t literalTripleTableID)
        : TableSchema{common::TableType::RDF, std::move(tableName), rdfID},
          resourceTableID{resourceTableID}, literalTableID{literalTabelID},
          resourceTripleTableID{resourceTripleTableID}, literalTripleTableID{literalTripleTableID} {
    }
    RdfGraphSchema(const RdfGraphSchema& other);

    bool isParent(common::table_id_t tableID) override;

    inline common::table_id_t getResourceTableID() const { return resourceTableID; }
    inline common::table_id_t getLiteralTableID() const { return literalTableID; }
    inline common::table_id_t getResourceTripleTableID() const { return resourceTripleTableID; }
    inline common::table_id_t getLiteralTripleTableID() const { return literalTripleTableID; }

    static std::unique_ptr<RdfGraphSchema> deserialize(common::Deserializer& deserializer);
    static std::string getResourceTableName(const std::string& graphName);
    static std::string getLiteralTableName(const std::string& graphName);
    static std::string getResourceTripleTableName(const std::string& graphName);
    static std::string getLiteralTripleTableName(const std::string& graphName);

    inline std::unique_ptr<TableSchema> copy() const final {
        return std::make_unique<RdfGraphSchema>(*this);
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
