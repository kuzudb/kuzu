#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"

#include <optional>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_set.h"
#include "common/keyword/rdf_keyword.h"
#include "common/serializer/deserializer.h"

namespace kuzu {
namespace catalog {

RDFGraphCatalogEntry::RDFGraphCatalogEntry(CatalogSet* set, std::string name,
    common::table_id_t resourceTableID, common::table_id_t literalTabelID,
    common::table_id_t resourceTripleTableID, common::table_id_t literalTripleTableID)
    : TableCatalogEntry{set, CatalogEntryType::RDF_GRAPH_ENTRY, std::move(name)},
      resourceTableID{resourceTableID}, literalTableID{literalTabelID},
      resourceTripleTableID{resourceTripleTableID}, literalTripleTableID{literalTripleTableID} {}

bool RDFGraphCatalogEntry::isParent(common::table_id_t tableID) {
    return tableID == resourceTableID || tableID == literalTableID ||
           tableID == resourceTripleTableID || tableID == literalTripleTableID;
}

common::table_id_set_t RDFGraphCatalogEntry::getTableIDSet() const {
    common::table_id_set_t result;
    result.insert(resourceTableID);
    result.insert(literalTableID);
    result.insert(resourceTripleTableID);
    result.insert(literalTripleTableID);
    return result;
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
    serializer.writeDebuggingInfo("resourceTableID");
    serializer.write(resourceTableID);
    serializer.writeDebuggingInfo("literalTableID");
    serializer.write(literalTableID);
    serializer.writeDebuggingInfo("resourceTripleTableID");
    serializer.write(resourceTripleTableID);
    serializer.writeDebuggingInfo("literalTripleTableID");
    serializer.write(literalTripleTableID);
}

std::unique_ptr<RDFGraphCatalogEntry> RDFGraphCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
    std::string debuggingInfo;
    common::table_id_t resourceTableID = common::INVALID_TABLE_ID;
    common::table_id_t literalTableID = common::INVALID_TABLE_ID;
    common::table_id_t resourceTripleTableID = common::INVALID_TABLE_ID;
    common::table_id_t literalTripleTableID = common::INVALID_TABLE_ID;
    deserializer.validateDebuggingInfo(debuggingInfo, "resourceTableID");
    deserializer.deserializeValue(resourceTableID);
    deserializer.validateDebuggingInfo(debuggingInfo, "literalTableID");
    deserializer.deserializeValue(literalTableID);
    deserializer.validateDebuggingInfo(debuggingInfo, "resourceTripleTableID");
    deserializer.deserializeValue(resourceTripleTableID);
    deserializer.validateDebuggingInfo(debuggingInfo, "literalTripleTableID");
    deserializer.deserializeValue(literalTripleTableID);
    auto rdfGraphEntry = std::make_unique<RDFGraphCatalogEntry>();
    rdfGraphEntry->resourceTableID = resourceTableID;
    rdfGraphEntry->literalTableID = literalTableID;
    rdfGraphEntry->resourceTripleTableID = resourceTripleTableID;
    rdfGraphEntry->literalTripleTableID = literalTripleTableID;
    return rdfGraphEntry;
}

std::unique_ptr<TableCatalogEntry> RDFGraphCatalogEntry::copy() const {
    auto other = std::make_unique<RDFGraphCatalogEntry>();
    other->resourceTableID = resourceTableID;
    other->literalTableID = literalTableID;
    other->resourceTripleTableID = resourceTripleTableID;
    other->literalTripleTableID = literalTripleTableID;
    other->copyFrom(*this);
    return other;
}

static std::optional<binder::BoundCreateTableInfo> getBoundCreateTableInfoForTable(
    transaction::Transaction* transaction, const CatalogEntrySet& entries,
    common::table_id_t tableID) {
    for (auto& [name, entry] : entries) {
        auto current = common::ku_dynamic_cast<TableCatalogEntry*>(entry);
        if (current->getTableID() == tableID) {
            auto boundInfo = current->getBoundCreateTableInfo(transaction);
            return boundInfo;
        }
    }
    return std::nullopt;
}

std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo>
RDFGraphCatalogEntry::getBoundExtraCreateInfo(transaction::Transaction* transaction) const {
    auto entries = set->getEntries(transaction);
    auto resourceTableBoundInfo =
        getBoundCreateTableInfoForTable(transaction, entries, resourceTableID);
    KU_ASSERT(resourceTableBoundInfo.has_value());
    auto literalTableBoundInfo =
        getBoundCreateTableInfoForTable(transaction, entries, literalTableID);
    KU_ASSERT(literalTableBoundInfo.has_value());
    auto resourceTripleTableBoundInfo =
        getBoundCreateTableInfoForTable(transaction, entries, resourceTripleTableID);
    KU_ASSERT(resourceTripleTableBoundInfo.has_value());
    auto literalTripleTableBoundInfo =
        getBoundCreateTableInfoForTable(transaction, entries, literalTripleTableID);
    KU_ASSERT(literalTripleTableBoundInfo.has_value());
    return std::make_unique<binder::BoundExtraCreateRdfGraphInfo>(
        std::move(resourceTableBoundInfo.value()), std::move(literalTableBoundInfo.value()),
        std::move(resourceTripleTableBoundInfo.value()),
        std::move(literalTripleTableBoundInfo.value()));
}

} // namespace catalog
} // namespace kuzu
