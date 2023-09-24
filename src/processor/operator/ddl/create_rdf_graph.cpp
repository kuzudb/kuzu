#include "processor/operator/ddl/create_rdf_graph.h"

#include "catalog/rdf_graph_schema.h"
#include "common/string_utils.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

void CreateRdfGraph::executeDDLInternal() {
    auto newRdfGraphID = catalog->addRdfGraphSchema(*info);
    auto writeCatalog = catalog->getWriteVersion();
    auto newRdfGraphSchema = (RdfGraphSchema*)writeCatalog->getTableSchema(newRdfGraphID);
    auto newNodeTableSchema =
        (NodeTableSchema*)writeCatalog->getTableSchema(newRdfGraphSchema->getNodeTableID());
    nodesStatistics->addNodeStatisticsAndDeletedIDs(newNodeTableSchema);
    auto newRelTableSchema =
        (RelTableSchema*)writeCatalog->getTableSchema(newRdfGraphSchema->getRelTableID());
    relsStatistics->addTableStatistic(newRelTableSchema);
    storageManager->getWAL()->logRdfGraphRecord(
        newRdfGraphID, newRdfGraphSchema->getNodeTableID(), newRdfGraphSchema->getRelTableID());
}

std::string CreateRdfGraph::getOutputMsg() {
    return common::StringUtils::string_format("RDF graph {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu
