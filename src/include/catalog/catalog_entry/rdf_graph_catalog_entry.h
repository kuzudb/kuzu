#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class RDFGraphCatalogEntry final : public TableCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    RDFGraphCatalogEntry() = default;
    RDFGraphCatalogEntry(CatalogSet* set, std::string name, common::table_id_t rdfID,
        common::table_id_t resourceTableID, common::table_id_t literalTabelID,
        common::table_id_t resourceTripleTableID, common::table_id_t literalTripleTableID);

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    bool isParent(common::table_id_t tableID) override;
    common::TableType getTableType() const override { return common::TableType::RDF; }
    common::table_id_t getResourceTableID() const { return resourceTableID; }
    common::table_id_t getLiteralTableID() const { return literalTableID; }
    common::table_id_t getResourceTripleTableID() const { return resourceTripleTableID; }
    common::table_id_t getLiteralTripleTableID() const { return literalTripleTableID; }
    common::table_id_set_t getTableIDSet() const;
    static std::string getResourceTableName(const std::string& graphName);
    static std::string getLiteralTableName(const std::string& graphName);
    static std::string getResourceTripleTableName(const std::string& graphName);
    static std::string getLiteralTripleTableName(const std::string& graphName);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RDFGraphCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::unique_ptr<TableCatalogEntry> copy() const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction* transaction) const override;

private:
    common::table_id_t resourceTableID;
    common::table_id_t literalTableID;
    common::table_id_t resourceTripleTableID;
    common::table_id_t literalTripleTableID;
};

} // namespace catalog
} // namespace kuzu
