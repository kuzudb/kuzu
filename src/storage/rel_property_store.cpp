#include "src/storage/include/rel_property_store.h"

namespace graphflow {
namespace storage {

RelPropertyStore::RelPropertyStore(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
    const string& directory, BufferManager& bufferManager) {
    propertyColumns.resize(catalog.getRelLabelsCount());
    for (auto relLabel = 0u; catalog.getRelLabelsCount(); relLabel++) {
        if (catalog.isSingleCaridinalityInDir(relLabel, FWD)) {
            initPropertyColumnsForRelLabel(
                catalog, numNodesPerLabel, directory, bufferManager, relLabel, FWD);
        } else if (catalog.isSingleCaridinalityInDir(relLabel, BWD)) {
            initPropertyColumnsForRelLabel(
                catalog, numNodesPerLabel, directory, bufferManager, relLabel, BWD);
        }
    }
}

void RelPropertyStore::initPropertyColumnsForRelLabel(Catalog& catalog,
    vector<uint64_t>& numNodesPerLabel, const string& directory, BufferManager& bufferManager,
    gfLabel_t relLabel, Direction dir) {
    propertyColumns[relLabel].resize(catalog.getNodeLabelsCount());
    for (auto& nodeLabel : catalog.getNodeLabelsForRelLabelDir(relLabel, dir)) {
        auto& propertyMap = catalog.getPropertyMapForRelLabel(relLabel);
        for (auto i = 0; i < propertyMap.size(); i++) {
            auto& property = propertyMap[i];
            auto fname =
                getRelPropertyColumnFname(directory, relLabel, nodeLabel, property.propertyName);
            switch (property.dataType) {
            case INT:
                propertyColumns[nodeLabel][i] = make_unique<ColumnInteger>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case DOUBLE:
                propertyColumns[nodeLabel][i] = make_unique<ColumnDouble>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            case BOOLEAN:
                propertyColumns[nodeLabel][i] = make_unique<ColumnBoolean>(
                    property.propertyName, fname, numNodesPerLabel[nodeLabel], bufferManager);
                break;
            default:
                throw std::invalid_argument("not supported.");
                propertyColumns[nodeLabel][i] = nullptr;
            }
        }
    }
}

} // namespace storage
} // namespace graphflow
