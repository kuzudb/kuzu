#include "processor/operator/ddl/create_rdf_graph.h"

#include "catalog/rdf_graph_schema.h"
#include "common/cast.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

void CreateRdfGraph::executeDDLInternal(ExecutionContext* context) {
    auto tx = context->clientContext->getTx();
    auto newRdfGraphID = catalog->addRdfGraphSchema(info);
    auto rdfGraphSchema = common::ku_dynamic_cast<TableSchema*, RdfGraphSchema*>(
        catalog->getTableSchema(tx, newRdfGraphID));
    auto resourceTableID = rdfGraphSchema->getResourceTableID();
    auto resourceTableSchema = common::ku_dynamic_cast<TableSchema*, NodeTableSchema*>(
        catalog->getTableSchema(tx, resourceTableID));
    nodesStatistics->addNodeStatisticsAndDeletedIDs(resourceTableSchema);
    storageManager->createTable(resourceTableID, catalog, tx);
    auto literalTableID = rdfGraphSchema->getLiteralTableID();
    auto literalTableSchema = common::ku_dynamic_cast<TableSchema*, NodeTableSchema*>(
        catalog->getTableSchema(tx, literalTableID));
    nodesStatistics->addNodeStatisticsAndDeletedIDs(literalTableSchema);
    storageManager->createTable(literalTableID, catalog, tx);
    auto resourceTripleTableID = rdfGraphSchema->getResourceTripleTableID();
    auto resourceTripleTableSchema = common::ku_dynamic_cast<TableSchema*, RelTableSchema*>(
        catalog->getTableSchema(tx, resourceTripleTableID));
    relsStatistics->addTableStatistic(resourceTripleTableSchema);
    storageManager->createTable(resourceTripleTableID, catalog, tx);
    auto literalTripleTableID = rdfGraphSchema->getLiteralTripleTableID();
    auto literalTripleTableSchema = common::ku_dynamic_cast<TableSchema*, RelTableSchema*>(
        catalog->getTableSchema(tx, literalTripleTableID));
    relsStatistics->addTableStatistic(literalTripleTableSchema);
    storageManager->createTable(literalTripleTableID, catalog, tx);
    storageManager->getWAL()->logRdfGraphRecord(newRdfGraphID, resourceTableID, literalTableID,
        resourceTripleTableID, literalTripleTableID);
}

std::string CreateRdfGraph::getOutputMsg() {
    return common::stringFormat("RDF graph {} has been created.", info.tableName);
}

} // namespace processor
} // namespace kuzu
