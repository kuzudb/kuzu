#include "processor/operator/ddl/create_rdf_graph.h"

#include "catalog/rdf_graph_schema.h"
#include "common/string_utils.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

void CreateRdfGraph::executeDDLInternal() {
    auto extraInfo = (binder::BoundExtraCreateRdfGraphInfo*)info->extraInfo.get();
    auto nodeInfo = extraInfo->nodeInfo.get();
    auto extraNodeInfo = (binder::BoundExtraCreateNodeTableInfo*)nodeInfo->extraInfo.get();
    for (auto& property : extraNodeInfo->properties) {
        property->setMetadataDAHInfo(
            storageManager->createMetadataDAHInfo(*property->getDataType()));
    }
    auto newRdfGraphID = catalog->addRdfGraphSchema(*info);
    auto writeCatalog = catalog->getWriteVersion();
    auto newRdfGraphSchema = (RdfGraphSchema*)writeCatalog->getTableSchema(newRdfGraphID);
    auto newNodeTableSchema =
        (NodeTableSchema*)writeCatalog->getTableSchema(newRdfGraphSchema->getNodeTableID());
    nodesStatistics->addNodeStatisticsAndDeletedIDs(newNodeTableSchema);
    auto newRelTableSchema =
        (RelTableSchema*)writeCatalog->getTableSchema(newRdfGraphSchema->getRelTableID());
    relsStatistics->addTableStatistic(newRelTableSchema);
}

std::string CreateRdfGraph::getOutputMsg() {
    return common::StringUtils::string_format("RDF graph {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu
