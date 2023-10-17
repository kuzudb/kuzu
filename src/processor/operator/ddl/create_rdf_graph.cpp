#include "processor/operator/ddl/create_rdf_graph.h"

#include "catalog/rdf_graph_schema.h"
#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

void CreateRdfGraph::executeDDLInternal() {
    auto newRdfGraphID = catalog->addRdfGraphSchema(*info);
    auto writeCatalog = catalog->getWriteVersion();
    auto rdfGraphSchema =
        reinterpret_cast<RdfGraphSchema*>(writeCatalog->getTableSchema(newRdfGraphID));
    auto resourceTableID = rdfGraphSchema->getResourceTableID();
    auto resourceTableSchema =
        reinterpret_cast<NodeTableSchema*>(writeCatalog->getTableSchema(resourceTableID));
    nodesStatistics->addNodeStatisticsAndDeletedIDs(resourceTableSchema);
    auto literalTableID = rdfGraphSchema->getLiteralTableID();
    auto literalTableSchema =
        reinterpret_cast<NodeTableSchema*>(writeCatalog->getTableSchema(literalTableID));
    nodesStatistics->addNodeStatisticsAndDeletedIDs(literalTableSchema);
    auto resourceTripleTableID = rdfGraphSchema->getResourceTripleTableID();
    auto resourceTripleTableSchema =
        reinterpret_cast<RelTableSchema*>(writeCatalog->getTableSchema(resourceTripleTableID));
    relsStatistics->addTableStatistic(resourceTripleTableSchema);
    auto literalTripleTableID = rdfGraphSchema->getLiteralTripleTableID();
    auto literalTripleTableSchema =
        reinterpret_cast<RelTableSchema*>(writeCatalog->getTableSchema(literalTripleTableID));
    relsStatistics->addTableStatistic(literalTripleTableSchema);
    storageManager->getWAL()->logRdfGraphRecord(newRdfGraphID, resourceTableID, literalTableID,
        resourceTripleTableID, literalTripleTableID);
}

std::string CreateRdfGraph::getOutputMsg() {
    return common::stringFormat("RDF graph {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu
