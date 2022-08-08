#include "src/processor/include/physical_plan/operator/copy_csv/copy_node_csv.h"

#include "src/processor/include/physical_plan/operator/copy_csv/in_mem_builder/in_mem_node_builder.h"

namespace graphflow {
namespace processor {

void CopyNodeCSV::execute(TaskScheduler& taskScheduler) {
    // InMemNodeBuilder needs to update the catalog and the unstructured property of the nodeLabel.
    catalog->initCatalogContentForWriteTrxIfNecessary();
    // We need to update nodesMetadata which contains the maxNodeOffsetInfo and
    // deletedNodeOffset.
    nodesMetadata->initNodeMetadataPerLabelForWriteTrxIfNecessaryNoLock();
    auto nodeBuilder =
        make_unique<InMemNodeBuilder>(csvDescription, outputDirectory, taskScheduler, *catalog);
    auto numNodesLoaded = nodeBuilder->load();
    (*(nodesMetadata->getNodesMetadataForWriteTrx()))[csvDescription.labelID]
        ->getNodeOffsetInfo()
        ->setMaxNodeOffset(numNodesLoaded - 1);
    wal->logCopyNodeCSVRecord(csvDescription.labelID);
}

} // namespace processor
} // namespace graphflow
