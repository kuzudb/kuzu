#include "storage/wal_replayer_utils.h"

#include "storage/index/hash_index_builder.h"

namespace kuzu {
namespace storage {

void WALReplayerUtils::createEmptyDBFilesForNewRelTable(Catalog* catalog, table_id_t tableID,
    const string& directory, const map<table_id_t, uint64_t>& maxNodeOffsetsPerTable) {
    auto relTableSchema = catalog->getReadOnlyVersion()->getRelTableSchema(tableID);
    for (auto relDirection : REL_DIRECTIONS) {
        auto boundTableIDs = catalog->getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
            tableID, relDirection);
        if (catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(tableID, relDirection)) {
            createEmptyDBFilesForColumns(
                boundTableIDs, maxNodeOffsetsPerTable, relDirection, directory, relTableSchema);
        } else {
            createEmptyDBFilesForLists(
                boundTableIDs, maxNodeOffsetsPerTable, relDirection, directory, relTableSchema);
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
    if (nodeTableSchema->getPrimaryKey().dataType.typeID == INT64) {
        auto pkIndex = make_unique<HashIndexBuilder<int64_t>>(
            StorageUtils::getNodeIndexFName(
                directory, nodeTableSchema->tableID, DBFileType::ORIGINAL),
            nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(0 /* numNodes */);
        pkIndex->flush();
    } else {
        auto pkIndex = make_unique<HashIndexBuilder<ku_string_t>>(
            StorageUtils::getNodeIndexFName(
                directory, nodeTableSchema->tableID, DBFileType::ORIGINAL),
            nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(0 /* numNodes */);
        pkIndex->flush();
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
        auto propertyID = relTableSchema->properties[i].propertyID;
        auto propertyDataType = relTableSchema->properties[i].dataType;
        if (isForRelPropertyColumn) {
            auto fName = StorageUtils::getRelPropertyColumnFName(directory, relTableSchema->tableID,
                tableID, relDirection, propertyID, DBFileType::ORIGINAL);
            InMemColumnFactory::getInMemPropertyColumn(fName, propertyDataType, numNodes)
                ->saveToFile();
        } else {
            auto fName = StorageUtils::getRelPropertyListsFName(directory, relTableSchema->tableID,
                tableID, relDirection, propertyID, DBFileType::ORIGINAL);
            auto inMemPropertyList =
                InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes);
            initLargeListPageListsAndSaveToFile(inMemPropertyList.get());
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForColumns(const unordered_set<table_id_t>& boundTableIDs,
    const map<table_id_t, uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
    const string& directory, RelTableSchema* relTableSchema) {
    for (auto boundTableID : boundTableIDs) {
        auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) == UINT64_MAX ?
                            0 :
                            maxNodeOffsetsPerTable.at(boundTableID) + 1;
        make_unique<InMemAdjColumn>(
            StorageUtils::getAdjColumnFName(directory, relTableSchema->tableID, boundTableID,
                relDirection, DBFileType::ORIGINAL),
            NodeIDCompressionScheme{relTableSchema->getUniqueNbrTableIDsForBoundTableIDDirection(
                relDirection, boundTableID)},
            numNodes)
            ->saveToFile();
        createEmptyDBFilesForRelProperties(relTableSchema, boundTableID, directory, relDirection,
            numNodes, true /* isForRelPropertyColumn */);
    }
}

void WALReplayerUtils::createEmptyDBFilesForLists(const unordered_set<table_id_t>& boundTableIDs,
    const map<table_id_t, uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
    const string& directory, RelTableSchema* relTableSchema) {
    for (auto boundTableID : boundTableIDs) {
        auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) == UINT64_MAX ?
                            0 :
                            maxNodeOffsetsPerTable.at(boundTableID) + 1;
        auto adjLists = make_unique<InMemAdjLists>(
            StorageUtils::getAdjListsFName(directory, relTableSchema->tableID, boundTableID,
                relDirection, DBFileType::ORIGINAL),
            NodeIDCompressionScheme{relTableSchema->getUniqueNbrTableIDsForBoundTableIDDirection(
                relDirection, boundTableID)},
            numNodes);
        initLargeListPageListsAndSaveToFile(adjLists.get());
        createEmptyDBFilesForRelProperties(relTableSchema, boundTableID, directory, relDirection,
            numNodes, false /* isForRelPropertyColumn */);
    }
}

void WALReplayerUtils::replaceOriginalColumnFilesWithWALVersionIfExists(
    string originalColFileName) {
    auto walColFileName = StorageUtils::appendWALFileSuffix(originalColFileName);
    FileUtils::renameFileIfExists(walColFileName, originalColFileName);
    // We also check if there is a WAL version of the overflow file for the column and if so
    // replace the original version.
    FileUtils::renameFileIfExists(StorageUtils::getOverflowFileName(walColFileName),
        StorageUtils::getOverflowFileName(originalColFileName));
}

void WALReplayerUtils::replaceOriginalListFilesWithWALVersionIfExists(string originalListFileName) {
    auto walListFileName = StorageUtils::appendWALFileSuffix(originalListFileName);
    FileUtils::renameFileIfExists(walListFileName, originalListFileName);
    FileUtils::renameFileIfExists(StorageUtils::getListMetadataFName(walListFileName),
        StorageUtils::getListMetadataFName(originalListFileName));
    // We also check if there is a WAL version of the overflow and header file for the list
    // and if so replace the original version.
    FileUtils::renameFileIfExists(StorageUtils::getOverflowFileName(walListFileName),
        StorageUtils::getOverflowFileName(originalListFileName));
    FileUtils::renameFileIfExists(StorageUtils::getListHeadersFName(walListFileName),
        StorageUtils::getListHeadersFName(originalListFileName));
}

void WALReplayerUtils::removeColumnFilesIfExists(string fileName) {
    FileUtils::removeFileIfExists(fileName);
    FileUtils::removeFileIfExists(StorageUtils::getOverflowFileName(fileName));
}

void WALReplayerUtils::removeListFilesIfExists(string fileName) {
    FileUtils::removeFileIfExists(fileName);
    FileUtils::removeFileIfExists(StorageUtils::getListMetadataFName(fileName));
    FileUtils::removeFileIfExists(StorageUtils::getOverflowFileName(fileName));
    FileUtils::removeFileIfExists(StorageUtils::getListHeadersFName(fileName));
}

void WALReplayerUtils::fileOperationOnNodeFiles(NodeTableSchema* nodeTableSchema, string directory,
    std::function<void(string fileName)> columnFileOperation,
    std::function<void(string fileName)> listFileOperation) {
    for (auto& property : nodeTableSchema->structuredProperties) {
        columnFileOperation(StorageUtils::getNodePropertyColumnFName(
            directory, nodeTableSchema->tableID, property.propertyID, DBFileType::ORIGINAL));
    }
    columnFileOperation(
        StorageUtils::getNodeIndexFName(directory, nodeTableSchema->tableID, DBFileType::ORIGINAL));
}

void WALReplayerUtils::fileOperationOnRelFiles(RelTableSchema* relTableSchema, string directory,
    const Catalog* catalog, std::function<void(string fileName)> columnFileOperation,
    std::function<void(string fileName)> listFileOperation) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto boundTableIDs = catalog->getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
            relTableSchema->tableID, relDirection);
        for (auto boundTableID : boundTableIDs) {
            auto isColumnProperty = catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(
                relTableSchema->tableID, relDirection);
            if (isColumnProperty) {
                columnFileOperation(StorageUtils::getAdjColumnFName(directory,
                    relTableSchema->tableID, boundTableID, relDirection, DBFileType::ORIGINAL));
            } else {
                listFileOperation(StorageUtils::getAdjListsFName(directory, relTableSchema->tableID,
                    boundTableID, relDirection, DBFileType::ORIGINAL));
            }
            fileOperationOnRelPropertyFiles(relTableSchema, boundTableID, directory, relDirection,
                isColumnProperty, columnFileOperation, listFileOperation);
        }
    }
}

void WALReplayerUtils::fileOperationOnRelPropertyFiles(RelTableSchema* tableSchema,
    table_id_t nodeTableID, const string& directory, RelDirection relDirection,
    bool isColumnProperty, std::function<void(string fileName)> columnFileOperation,
    std::function<void(string fileName)> listFileOperation) {
    for (auto i = 0u; i < tableSchema->getNumProperties(); ++i) {
        auto property = tableSchema->properties[i];
        auto propertyID = property.propertyID;
        if (isColumnProperty) {
            columnFileOperation(StorageUtils::getRelPropertyColumnFName(directory,
                tableSchema->tableID, nodeTableID, relDirection, propertyID, DBFileType::ORIGINAL));
        } else {
            listFileOperation(StorageUtils::getRelPropertyListsFName(directory,
                tableSchema->tableID, nodeTableID, relDirection, propertyID, DBFileType::ORIGINAL));
        }
    }
}

} // namespace storage
} // namespace kuzu
