#include "src/storage/include/wal_replayer_utils.h"

#include "src/storage/index/include/in_mem_hash_index.h"

namespace graphflow {
namespace storage {

void WALReplayerUtils::createEmptyDBFilesForNewRelTable(Catalog* catalog, table_id_t tableID,
    const string& directory, const vector<uint64_t>& maxNodeOffsetsPerTable) {
    auto relTableSchema = catalog->getReadOnlyVersion()->getRelTableSchema(tableID);
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeTableIDs = catalog->getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
            tableID, relDirection);
        auto directionNodeIDCompressionScheme = NodeIDCompressionScheme(
            catalog->getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
                tableID, !relDirection),
            maxNodeOffsetsPerTable);
        if (catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(tableID, relDirection)) {
            createEmptyDBFilesForColumns(nodeTableIDs, maxNodeOffsetsPerTable, relDirection,
                directory, directionNodeIDCompressionScheme, relTableSchema);
        } else {
            createEmptyDBFilesForLists(nodeTableIDs, maxNodeOffsetsPerTable, relDirection,
                directory, directionNodeIDCompressionScheme, relTableSchema);
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForNewNodeTable(
    Catalog* catalog, table_id_t tableID, string directory) {
    auto nodeTableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(tableID);
    for (auto& property : nodeTableSchema->structuredProperties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(
            directory, nodeTableSchema->tableID, property.propertyID, DBFileType::ORIGINAL);
        InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, 0 /* numNodes */)
            ->saveToFile();
    }
    auto unstrPropertyLists =
        make_unique<InMemUnstructuredLists>(StorageUtils::getNodeUnstrPropertyListsFName(directory,
                                                nodeTableSchema->tableID, DBFileType::ORIGINAL),
            0 /* numNodes */);
    initLargeListPageListsAndSaveToFile(unstrPropertyLists.get());
    auto IDIndex = make_unique<InMemHashIndex>(
        StorageUtils::getNodeIndexFName(directory, nodeTableSchema->tableID, DBFileType::ORIGINAL),
        nodeTableSchema->getPrimaryKey().dataType);
    IDIndex->bulkReserve(0 /* numNodes */);
    IDIndex->flush();
}

void WALReplayerUtils::replaceNodeFilesWithVersionFromWALIfExists(
    catalog::NodeTableSchema* nodeTableSchema, string directory) {
    for (auto& property : nodeTableSchema->structuredProperties) {
        replaceOriginalColumnFilesWithWALVersionIfExists(StorageUtils::getNodePropertyColumnFName(
            directory, nodeTableSchema->tableID, property.propertyID, DBFileType::ORIGINAL));
    }
    replaceOriginalListFilesWithWALVersionIfExists(StorageUtils::getNodeUnstrPropertyListsFName(
        directory, nodeTableSchema->tableID, DBFileType::ORIGINAL));
    replaceOriginalColumnFilesWithWALVersionIfExists(
        StorageUtils::getNodeIndexFName(directory, nodeTableSchema->tableID, DBFileType::ORIGINAL));
}

void WALReplayerUtils::replaceRelPropertyFilesWithVersionFromWALIfExists(
    catalog::RelTableSchema* relTableSchema, string directory, const catalog::Catalog* catalog) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto nodeTableIDs = catalog->getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
            relTableSchema->tableID, relDirection);
        for (auto nodeTableID : nodeTableIDs) {
            auto isColumnProperty = catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(
                relTableSchema->tableID, relDirection);
            if (isColumnProperty) {
                replaceOriginalColumnFilesWithWALVersionIfExists(
                    StorageUtils::getAdjColumnFName(directory, relTableSchema->tableID, nodeTableID,
                        relDirection, DBFileType::ORIGINAL));
            } else {
                replaceOriginalListFilesWithWALVersionIfExists(
                    StorageUtils::getAdjListsFName(directory, relTableSchema->tableID, nodeTableID,
                        relDirection, DBFileType::ORIGINAL));
            }
            replaceRelPropertyFilesWithVersionFromWAL(
                relTableSchema, nodeTableID, directory, relDirection, isColumnProperty);
        }
    }
}

void WALReplayerUtils::initLargeListPageListsAndSaveToFile(InMemLists* inMemLists) {
    inMemLists->getListsMetadataBuilder()->initLargeListPageLists(0 /* largeListIdx */);
    inMemLists->saveToFile();
}

void WALReplayerUtils::createEmptyDBFilesForRelProperties(RelTableSchema* relTableSchema,
    table_id_t tableID, const string& directory, RelDirection relDirection, uint32_t numNodes,
    bool isForRelPropertyColumn) {
    for (auto i = 0u; i < relTableSchema->getNumProperties(); ++i) {
        auto propertyName = relTableSchema->properties[i].name;
        auto propertyDataType = relTableSchema->properties[i].dataType;
        if (isForRelPropertyColumn) {
            auto fName = StorageUtils::getRelPropertyColumnFName(directory, relTableSchema->tableID,
                tableID, relDirection, propertyName, DBFileType::ORIGINAL);
            InMemColumnFactory::getInMemPropertyColumn(fName, propertyDataType, numNodes)
                ->saveToFile();
        } else {
            auto fName =
                StorageUtils::getRelPropertyListsFName(directory, relTableSchema->tableID, tableID,
                    relDirection, relTableSchema->properties[i].propertyID, DBFileType::ORIGINAL);
            auto inMemPropertyList =
                InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes);
            initLargeListPageListsAndSaveToFile(inMemPropertyList.get());
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForColumns(const unordered_set<table_id_t>& nodeTableIDs,
    const vector<uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
    const string& directory, const NodeIDCompressionScheme& directionNodeIDCompressionScheme,
    RelTableSchema* relTableSchema) {
    for (auto nodeTableID : nodeTableIDs) {
        auto numNodes = maxNodeOffsetsPerTable[nodeTableID] == UINT64_MAX ?
                            0 :
                            maxNodeOffsetsPerTable[nodeTableID] + 1;
        make_unique<InMemAdjColumn>(
            StorageUtils::getAdjColumnFName(directory, relTableSchema->tableID, nodeTableID,
                relDirection, DBFileType::ORIGINAL),
            directionNodeIDCompressionScheme, numNodes)
            ->saveToFile();
        createEmptyDBFilesForRelProperties(relTableSchema, nodeTableID, directory, relDirection,
            numNodes, true /* isForRelPropertyColumn */);
    }
}

void WALReplayerUtils::createEmptyDBFilesForLists(const unordered_set<table_id_t>& nodeTableIDs,
    const vector<uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
    const string& directory, const NodeIDCompressionScheme& directionNodeIDCompressionScheme,
    RelTableSchema* relTableSchema) {
    for (auto nodeTableID : nodeTableIDs) {
        auto numNodes = maxNodeOffsetsPerTable[nodeTableID] == UINT64_MAX ?
                            0 :
                            maxNodeOffsetsPerTable[nodeTableID] + 1;
        auto adjLists = make_unique<InMemAdjLists>(
            StorageUtils::getAdjListsFName(directory, relTableSchema->tableID, nodeTableID,
                relDirection, DBFileType::ORIGINAL),
            directionNodeIDCompressionScheme, numNodes);
        initLargeListPageListsAndSaveToFile(adjLists.get());
        createEmptyDBFilesForRelProperties(relTableSchema, nodeTableID, directory, relDirection,
            numNodes, false /* isForRelPropertyColumn */);
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

void WALReplayerUtils::replaceRelPropertyFilesWithVersionFromWAL(
    catalog::RelTableSchema* relTableSchema, table_id_t tableID, const string& directory,
    RelDirection relDirection, bool isColumnProperty) {
    for (auto i = 0u; i < relTableSchema->getNumProperties(); ++i) {
        auto property = relTableSchema->properties[i];
        if (isColumnProperty) {
            replaceOriginalColumnFilesWithWALVersionIfExists(
                StorageUtils::getRelPropertyColumnFName(directory, relTableSchema->tableID, tableID,
                    relDirection, property.name, DBFileType::ORIGINAL));
        } else {
            replaceOriginalListFilesWithWALVersionIfExists(
                StorageUtils::getRelPropertyListsFName(directory, relTableSchema->tableID, tableID,
                    relDirection, relTableSchema->properties[i].propertyID, DBFileType::ORIGINAL));
        }
    }
}

} // namespace storage
} // namespace graphflow
