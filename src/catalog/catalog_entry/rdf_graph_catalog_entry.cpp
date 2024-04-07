#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"

#include "common/keyword/rdf_keyword.h"

namespace kuzu {
namespace catalog {

RDFGraphCatalogEntry::RDFGraphCatalogEntry(std::string name, common::table_id_t rdfID,
    common::table_id_t resourceTableID, common::table_id_t literalTabelID,
    common::table_id_t resourceTripleTableID, common::table_id_t literalTripleTableID)
    : TableCatalogEntry{CatalogEntryType::RDF_GRAPH_ENTRY, std::move(name), rdfID},
      resourceTableID{resourceTableID}, literalTableID{literalTabelID},
      resourceTripleTableID{resourceTripleTableID}, literalTripleTableID{literalTripleTableID} {}

RDFGraphCatalogEntry::RDFGraphCatalogEntry(const RDFGraphCatalogEntry& other)
    : TableCatalogEntry{other} {
    resourceTableID = other.resourceTableID;
    literalTableID = other.literalTableID;
    resourceTripleTableID = other.resourceTripleTableID;
    literalTripleTableID = other.literalTripleTableID;
}

bool RDFGraphCatalogEntry::isParent(common::table_id_t tableID) {
    return tableID == resourceTableID || tableID == literalTableID ||
           tableID == resourceTripleTableID || tableID == literalTripleTableID;
}

std::string RDFGraphCatalogEntry::getResourceTableName(const std::string& graphName) {
    return graphName + std::string(common::rdf::RESOURCE_TABLE_SUFFIX);
}

std::string RDFGraphCatalogEntry::getLiteralTableName(const std::string& graphName) {
    return graphName + std::string(common::rdf::LITERAL_TABLE_SUFFIX);
}

std::string RDFGraphCatalogEntry::getResourceTripleTableName(const std::string& graphName) {
    return graphName + std::string(common::rdf::RESOURCE_TRIPLE_TABLE_SUFFIX);
}

std::string RDFGraphCatalogEntry::getLiteralTripleTableName(const std::string& graphName) {
    return graphName + std::string(common::rdf::LITERAL_TRIPLE_TABLE_SUFFIX);
}

void RDFGraphCatalogEntry::serialize(common::Serializer& serializer) const {
    TableCatalogEntry::serialize(serializer);
    serializer.write(resourceTableID);
    serializer.write(literalTableID);
    serializer.write(resourceTripleTableID);
    serializer.write(literalTripleTableID);
}

std::unique_ptr<RDFGraphCatalogEntry> RDFGraphCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    common::table_id_t resourceTableID;
    common::table_id_t literalTableID;
    common::table_id_t resourceTripleTableID;
    common::table_id_t literalTripleTableID;
    deserializer.deserializeValue(resourceTableID);
    deserializer.deserializeValue(literalTableID);
    deserializer.deserializeValue(resourceTripleTableID);
    deserializer.deserializeValue(literalTripleTableID);
    auto rdfGraphEntry = std::make_unique<RDFGraphCatalogEntry>();
    rdfGraphEntry->resourceTableID = resourceTableID;
    rdfGraphEntry->literalTableID = literalTableID;
    rdfGraphEntry->resourceTripleTableID = resourceTripleTableID;
    rdfGraphEntry->literalTripleTableID = literalTripleTableID;
    return rdfGraphEntry;
}

std::unique_ptr<CatalogEntry> RDFGraphCatalogEntry::copy() const {
    return std::make_unique<RDFGraphCatalogEntry>(*this);
}

} // namespace catalog
} // namespace kuzu
