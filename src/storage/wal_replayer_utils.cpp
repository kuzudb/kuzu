#include "src/storage/include/wal_replayer_utils.h"

#include "src/storage/index/include/in_mem_hash_index.h"

namespace graphflow {
namespace storage {

void WALReplayerUtils::createEmptyDBFilesForNewRelTable(Catalog* catalog, label_t labelID,
    const string& directory, const vector<uint64_t>& maxNodeOffsetsPerLabel) {
    auto relLabel = catalog->getReadOnlyVersion()->getRelLabel(labelID);
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeLabels =
            catalog->getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(labelID, relDirection);
        auto directionNodeIDCompressionScheme = NodeIDCompressionScheme(
            catalog->getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
                labelID, !relDirection),
            maxNodeOffsetsPerLabel);
        if (catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(labelID, relDirection)) {
            createEmptyDBFilesForColumns(nodeLabels, maxNodeOffsetsPerLabel, relDirection,
                directory, directionNodeIDCompressionScheme, relLabel);
        } else {
            createEmptyDBFilesForLists(nodeLabels, maxNodeOffsetsPerLabel, relDirection, directory,
                directionNodeIDCompressionScheme, relLabel);
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForNewNodeTable(
    Catalog* catalog, label_t label, string directory) {
    auto nodeLabel = catalog->getReadOnlyVersion()->getNodeLabel(label);
    for (auto& property : nodeLabel->structuredProperties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(
            directory, nodeLabel->labelId, property.propertyID, false /* isForWALRecord */);
        loader::InMemColumnFactory::getInMemPropertyColumn(
            fName, property.dataType, 0 /* numNodes */)
            ->saveToFile();
    }
    auto unstrPropertyLists = make_unique<loader::InMemUnstructuredLists>(
        StorageUtils::getNodeUnstrPropertyListsFName(
            directory, nodeLabel->labelId, false /* isForWALRecord */),
        0 /* numNodes */);
    initLargeListPageListsAndSaveToFile(unstrPropertyLists.get());
    auto IDIndex = make_unique<InMemHashIndex>(
        StorageUtils::getNodeIndexFName(directory, nodeLabel->labelId, false /* isForWALRecord */),
        nodeLabel->getPrimaryKey().dataType);
    IDIndex->bulkReserve(0 /* numNodes */);
    IDIndex->flush();
}

void WALReplayerUtils::initLargeListPageListsAndSaveToFile(loader::InMemLists* inMemLists) {
    inMemLists->getListsMetadataBuilder()->initLargeListPageLists(0 /* largeListIdx */);
    inMemLists->saveToFile();
}

void WALReplayerUtils::createEmptyDBFilesForRelProperties(RelLabel* relLabel, label_t nodeLabel,
    const string& directory, RelDirection relDirection, uint32_t numNodes,
    bool isForRelPropertyColumn) {
    for (auto i = 0u; i < relLabel->getNumProperties(); ++i) {
        auto propertyName = relLabel->properties[i].name;
        auto propertyDataType = relLabel->properties[i].dataType;
        if (isForRelPropertyColumn) {
            auto fName = StorageUtils::getRelPropertyColumnFName(directory, relLabel->labelId,
                nodeLabel, relDirection, propertyName, false /* isForWALRecord */);
            loader::InMemColumnFactory::getInMemPropertyColumn(fName, propertyDataType, numNodes)
                ->saveToFile();
        } else {
            auto fName =
                StorageUtils::getRelPropertyListsFName(directory, relLabel->labelId, nodeLabel,
                    relDirection, relLabel->properties[i].propertyID, false /* isForWALRecord */);
            auto inMemPropertyList =
                loader::InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes);
            initLargeListPageListsAndSaveToFile(inMemPropertyList.get());
            auto exists = FileUtils::fileExists(fName);
            auto a = 2;
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForColumns(const unordered_set<label_t>& nodeLabels,
    const vector<uint64_t>& maxNodeOffsetsPerLabel, RelDirection relDirection,
    const string& directory, const NodeIDCompressionScheme& directionNodeIDCompressionScheme,
    RelLabel* relLabel) {
    for (auto nodeLabel : nodeLabels) {
        auto numNodes = maxNodeOffsetsPerLabel[nodeLabel] == UINT64_MAX ?
                            0 :
                            maxNodeOffsetsPerLabel[nodeLabel] + 1;
        make_unique<loader::InMemAdjColumn>(
            StorageUtils::getAdjColumnFName(
                directory, relLabel->labelId, nodeLabel, relDirection, false /* isForWALRecord */),
            directionNodeIDCompressionScheme, numNodes)
            ->saveToFile();
        createEmptyDBFilesForRelProperties(relLabel, nodeLabel, directory, relDirection, numNodes,
            true /* isForRelPropertyColumn */);
    }
}

void WALReplayerUtils::createEmptyDBFilesForLists(const unordered_set<label_t>& nodeLabels,
    const vector<uint64_t>& maxNodeOffsetsPerLabel, RelDirection relDirection,
    const string& directory, const NodeIDCompressionScheme& directionNodeIDCompressionScheme,
    RelLabel* relLabel) {
    for (auto nodeLabel : nodeLabels) {
        auto numNodes = maxNodeOffsetsPerLabel[nodeLabel] == UINT64_MAX ?
                            0 :
                            maxNodeOffsetsPerLabel[nodeLabel] + 1;
        auto adjLists = make_unique<loader::InMemAdjLists>(
            StorageUtils::getAdjListsFName(
                directory, relLabel->labelId, nodeLabel, relDirection, false /* isForWALRecord */),
            directionNodeIDCompressionScheme, numNodes);
        initLargeListPageListsAndSaveToFile(adjLists.get());
        createEmptyDBFilesForRelProperties(relLabel, nodeLabel, directory, relDirection, numNodes,
            false /* isForRelPropertyColumn */);
    }
}

void WALReplayerUtils::overwriteNodeColumnAndListFilesWithVersionFromWAL(
    catalog::NodeLabel* nodeLabel, string directory) {
    for (auto& property : nodeLabel->structuredProperties) {
        FileUtils::overwriteFile(
            StorageUtils::getNodePropertyColumnFName(
                directory, nodeLabel->labelId, property.propertyID, true /* isForWALRecord */),
            StorageUtils::getNodePropertyColumnFName(
                directory, nodeLabel->labelId, property.propertyID, false /* isForWALRecord */));
    }
    FileUtils::overwriteFile(StorageUtils::getNodeUnstrPropertyListsFName(
                                 directory, nodeLabel->labelId, true /* isForWALRecord */),
        StorageUtils::getNodeUnstrPropertyListsFName(
            directory, nodeLabel->labelId, false /* isForWALRecord */));
}

} // namespace storage
} // namespace graphflow
