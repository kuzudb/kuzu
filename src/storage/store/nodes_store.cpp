#include "src/storage/store/include/nodes_store.h"

namespace graphflow {
namespace storage {

NodesStore::NodesStore(const Catalog& catalog, BufferManager& bufferManager,
    const string& directory, bool isInMemoryMode, WAL* wal)
    : nodesMetadata(directory) {
    nodeTables.resize(catalog.getNumNodeLabels());
    for (auto label = 0u; label < catalog.getNumNodeLabels(); label++) {
        nodeTables[label] = make_unique<NodeTable>(nodesMetadata.getNodeMetadata(label),
            bufferManager, isInMemoryMode, catalog.getAllNodeProperties(label), directory, wal);
    }
}

void NodesStore::addNode(const Catalog& catalog, const string& labelName,
    BufferManager* bufferManager, bool isInMemory, const string& directory, WAL* wal) {
    auto nodeLabel = catalog.getNodeLabelFromName(labelName);
    auto properties = catalog.getStructuredNodeProperties(nodeLabel);
    auto primaryKey = catalog.getNodePrimaryKeyProperty(nodeLabel);
    nodesMetadata.addNode(nodeLabel, primaryKey.name);
    createColumns(directory, nodeLabel, properties);
    createHashIndex(directory, nodeLabel, primaryKey.dataType);
    nodeTables.push_back(make_unique<NodeTable>(nodesMetadata.getNodeMetadata(nodeLabel),
        *bufferManager, isInMemory, move(properties), move(directory), wal));
}

void NodesStore::createColumns(
    const string& directory, label_t nodeLabel, vector<Property> properties) {
    for (auto property : properties) {
        graphflow::loader::InMemColumnFactory::getInMemPropertyColumn(
            StorageUtils::getNodePropertyColumnFName(directory, nodeLabel, property.name),
            property.dataType, 0 /* numNodes */)
            ->saveToFile();
    }
}

void NodesStore::createHashIndex(const string& directory, label_t nodeLabel, DataType pkDataType) {
    auto pkIndex = make_unique<InMemHashIndex>(
        StorageUtils::getNodeIndexFName(directory, nodeLabel), pkDataType);
    pkIndex->bulkReserve(0);
    pkIndex->flush();
}

} // namespace storage
} // namespace graphflow
