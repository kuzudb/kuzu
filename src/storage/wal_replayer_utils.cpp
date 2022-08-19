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
            directory, nodeLabel->labelID, property.propertyID, DBFileType::ORIGINAL);
        InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, 0 /* numNodes */)
            ->saveToFile();
    }
    auto unstrPropertyLists =
        make_unique<InMemUnstructuredLists>(StorageUtils::getNodeUnstrPropertyListsFName(directory,
                                                nodeLabel->labelID, DBFileType::ORIGINAL),
            0 /* numNodes */);
    initLargeListPageListsAndSaveToFile(unstrPropertyLists.get());
    auto IDIndex = make_unique<InMemHashIndex>(
        StorageUtils::getNodeIndexFName(directory, nodeLabel->labelID, DBFileType::ORIGINAL),
        nodeLabel->getPrimaryKey().dataType);
    IDIndex->bulkReserve(0 /* numNodes */);
    IDIndex->flush();
}

void WALReplayerUtils::replaceNodeFilesWithVersionFromWALIfExists(
    catalog::NodeLabel* nodeLabel, string directory) {
    for (auto& property : nodeLabel->structuredProperties) {
        replaceOriginalColumnFilesWithWALVersionIfExists(StorageUtils::getNodePropertyColumnFName(
            directory, nodeLabel->labelID, property.propertyID, DBFileType::ORIGINAL));
    }
    replaceOriginalListFilesWithWALVersionIfExists(StorageUtils::getNodeUnstrPropertyListsFName(
        directory, nodeLabel->labelID, DBFileType::ORIGINAL));
    replaceOriginalColumnFilesWithWALVersionIfExists(
        StorageUtils::getNodeIndexFName(directory, nodeLabel->labelID, DBFileType::ORIGINAL));
}

void WALReplayerUtils::replaceRelPropertyFilesWithVersionFromWALIfExists(
    catalog::RelLabel* relLabel, string directory, const catalog::Catalog* catalog) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeLabels = catalog->getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
            relLabel->labelID, relDirection);
        for (auto nodeLabel : nodeLabels) {
            auto isColumnProperty = catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(
                relLabel->labelID, relDirection);
            if (isColumnProperty) {
                replaceOriginalColumnFilesWithWALVersionIfExists(StorageUtils::getAdjColumnFName(
                    directory, relLabel->labelID, nodeLabel, relDirection, DBFileType::ORIGINAL));
            } else {
                replaceOriginalListFilesWithWALVersionIfExists(StorageUtils::getAdjListsFName(
                    directory, relLabel->labelID, nodeLabel, relDirection, DBFileType::ORIGINAL));
            }
            replaceRelPropertyFilesWithVersionFromWAL(
                relLabel, nodeLabel, directory, relDirection, isColumnProperty);
        }
    }
}

void WALReplayerUtils::initLargeListPageListsAndSaveToFile(InMemLists* inMemLists) {
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
            auto fName = StorageUtils::getRelPropertyColumnFName(directory, relLabel->labelID,
                nodeLabel, relDirection, propertyName, DBFileType::ORIGINAL);
            InMemColumnFactory::getInMemPropertyColumn(fName, propertyDataType, numNodes)
                ->saveToFile();
        } else {
            auto fName = StorageUtils::getRelPropertyListsFName(directory, relLabel->labelID,
                nodeLabel, relDirection, relLabel->properties[i].propertyID, DBFileType::ORIGINAL);
            auto inMemPropertyList =
                InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes);
            initLargeListPageListsAndSaveToFile(inMemPropertyList.get());
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
        make_unique<InMemAdjColumn>(StorageUtils::getAdjColumnFName(directory, relLabel->labelID,
                                        nodeLabel, relDirection, DBFileType::ORIGINAL),
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
        auto adjLists =
            make_unique<InMemAdjLists>(StorageUtils::getAdjListsFName(directory, relLabel->labelID,
                                           nodeLabel, relDirection, DBFileType::ORIGINAL),
                directionNodeIDCompressionScheme, numNodes);
        initLargeListPageListsAndSaveToFile(adjLists.get());
        createEmptyDBFilesForRelProperties(relLabel, nodeLabel, directory, relDirection, numNodes,
            false /* isForRelPropertyColumn */);
    }
}

void WALReplayerUtils::replaceOriginalColumnFilesWithWALVersionIfExists(
    string originalColFileName) {
    auto walColFileName = StorageUtils::appendWALFileSuffix(originalColFileName);
    FileUtils::renameFileIfExists(walColFileName, originalColFileName);
    // We also check if there is a WAL version of the overflow file for the column and if so
    // replace the original version.
    FileUtils::renameFileIfExists(StorageUtils::getOverflowPagesFName(walColFileName),
        StorageUtils::getOverflowPagesFName(originalColFileName));
}

void WALReplayerUtils::replaceOriginalListFilesWithWALVersionIfExists(string originalListFileName) {
    auto walListFileName = StorageUtils::appendWALFileSuffix(originalListFileName);
    FileUtils::renameFileIfExists(walListFileName, originalListFileName);
    FileUtils::renameFileIfExists(StorageUtils::getListMetadataFName(walListFileName),
        StorageUtils::getListMetadataFName(originalListFileName));
    // We also check if there is a WAL version of the overflow and header file for the list
    // and if so replace the original version.
    FileUtils::renameFileIfExists(StorageUtils::getOverflowPagesFName(walListFileName),
        StorageUtils::getOverflowPagesFName(originalListFileName));
    FileUtils::renameFileIfExists(StorageUtils::getListHeadersFName(walListFileName),
        StorageUtils::getListHeadersFName(originalListFileName));
}

void WALReplayerUtils::replaceRelPropertyFilesWithVersionFromWAL(catalog::RelLabel* relLabel,
    label_t nodeLabel, const string& directory, RelDirection relDirection, bool isColumnProperty) {
    for (auto i = 0u; i < relLabel->getNumProperties(); ++i) {
        auto property = relLabel->properties[i];
        if (isColumnProperty) {
            replaceOriginalColumnFilesWithWALVersionIfExists(
                StorageUtils::getRelPropertyColumnFName(directory, relLabel->labelID, nodeLabel,
                    relDirection, property.name, DBFileType::ORIGINAL));
        } else {
            replaceOriginalListFilesWithWALVersionIfExists(
                StorageUtils::getRelPropertyListsFName(directory, relLabel->labelID, nodeLabel,
                    relDirection, relLabel->properties[i].propertyID, DBFileType::ORIGINAL));
        }
    }
}

} // namespace storage
} // namespace graphflow
