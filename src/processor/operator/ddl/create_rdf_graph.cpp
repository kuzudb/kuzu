#include "processor/operator/ddl/create_rdf_graph.h"

#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "common/cast.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

using namespace kuzu::common;

void CreateRdfGraph::executeDDLInternal(ExecutionContext* context) {
    auto tx = context->clientContext->getTx();
    auto newRdfGraphID = catalog->addRdfGraphSchema(info);
    auto rdfGraphEntry = common::ku_dynamic_cast<TableCatalogEntry*, RDFGraphCatalogEntry*>(
        catalog->getTableCatalogEntry(tx, newRdfGraphID));
    auto resourceTableID = rdfGraphEntry->getResourceTableID();
    auto resourceTableEntry = common::ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(
        catalog->getTableCatalogEntry(tx, resourceTableID));
    nodesStatistics->addNodeStatisticsAndDeletedIDs(resourceTableEntry);
    storageManager->createTable(resourceTableID, catalog, tx);
    auto literalTableID = rdfGraphEntry->getLiteralTableID();
    auto literalTableEntry = common::ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(
        catalog->getTableCatalogEntry(tx, literalTableID));
    nodesStatistics->addNodeStatisticsAndDeletedIDs(literalTableEntry);
    storageManager->createTable(literalTableID, catalog, tx);
    auto resourceTripleTableID = rdfGraphEntry->getResourceTripleTableID();
    auto resourceTripleTableEntry =
        common::ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
            catalog->getTableCatalogEntry(tx, resourceTripleTableID));
    relsStatistics->addTableStatistic(resourceTripleTableEntry);
    storageManager->createTable(resourceTripleTableID, catalog, tx);
    auto literalTripleTableID = rdfGraphEntry->getLiteralTripleTableID();
    auto literalTripleTableEntry =
        common::ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
            catalog->getTableCatalogEntry(tx, literalTripleTableID));
    relsStatistics->addTableStatistic(literalTripleTableEntry);
    storageManager->createTable(literalTripleTableID, catalog, tx);
    storageManager->getWAL()->logRdfGraphRecord(newRdfGraphID, resourceTableID, literalTableID,
        resourceTripleTableID, literalTripleTableID);
}

std::string CreateRdfGraph::getOutputMsg() {
    return common::stringFormat("RDF graph {} has been created.", info.tableName);
}

} // namespace processor
} // namespace kuzu
