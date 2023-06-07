#include "processor/operator/ddl/create_rdf_graph.h"

#include "common/string_utils.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

void CreateRDFGraph::executeDDLInternal() {
    auto stringType = std::make_unique<common::LogicalType>(common::LogicalTypeID::STRING);
    auto rdfResourceIRIProperty = std::make_unique<Property>(
        RDFConstants::RDF_GRAPH_IRI_PROPERTY_NAME, std::move(stringType));
    rdfResourceIRIProperty->setMetadataDAHInfo(
        storageManager.createMetadataDAHInfo(*rdfResourceIRIProperty->getDataType()));
    auto newRDFGraphID =
        catalog->addRDFGraphSchema(rdfGraphName, std::move(rdfResourceIRIProperty));
    auto resourceNodeTableSchema =
        catalog->getWriteVersion()->getRDFGraphNodeTableSchema(newRDFGraphID);
    resourcesNodesStatistics->addNodeStatisticsAndDeletedIDs(resourceNodeTableSchema);
    triplesRelsStatistics->addTableStatistic(
        catalog->getWriteVersion()->getRDFGraphRelTableSchema(newRDFGraphID));
}

std::string CreateRDFGraph::getOutputMsg() {
    return StringUtils::string_format("RdfGraph: {} has been created.", rdfGraphName);
}

} // namespace processor
} // namespace kuzu
