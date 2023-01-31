#include "storage/wal_replayer_utils.h"

#include "storage/index/hash_index_builder.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void WALReplayerUtils::removeDBFilesForRelProperty(
    const std::string& directory, RelTableSchema* relTableSchema, property_id_t propertyID) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto boundTableID = relTableSchema->getBoundTableID(relDirection);
        if (relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
            removeColumnFilesForPropertyIfExists(directory, relTableSchema->tableID, boundTableID,
                relDirection, propertyID, DBFileType::WAL_VERSION);
        } else {
            removeListFilesForPropertyIfExists(directory, relTableSchema->tableID, boundTableID,
                relDirection, propertyID, DBFileType::WAL_VERSION);
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForNewRelTable(RelTableSchema* relTableSchema,
    const std::string& directory, const std::map<table_id_t, uint64_t>& maxNodeOffsetsPerTable) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto boundTableID = relTableSchema->getBoundTableID(relDirection);
        if (relTableSchema->isSingleMultiplicityInDirection(relDirection)) {
            createEmptyDBFilesForColumns(
                maxNodeOffsetsPerTable, relDirection, directory, relTableSchema);
        } else {
            createEmptyDBFilesForLists(
                maxNodeOffsetsPerTable, relDirection, directory, relTableSchema);
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForNewNodeTable(
    NodeTableSchema* nodeTableSchema, const std::string& directory) {
    for (auto& property : nodeTableSchema->properties) {
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

void WALReplayerUtils::renameDBFilesForRelProperty(const std::string& directory,
    kuzu::catalog::RelTableSchema* relTableSchema, kuzu::common::property_id_t propertyID) {
    for (auto direction : REL_DIRECTIONS) {
        auto boundTableID = relTableSchema->getBoundTableID(direction);
        if (relTableSchema->isSingleMultiplicityInDirection(direction)) {
            replaceOriginalColumnFilesWithWALVersionIfExists(
                StorageUtils::getRelPropertyColumnFName(directory, relTableSchema->tableID,
                    direction, propertyID, DBFileType::ORIGINAL));
        } else {
            replaceOriginalListFilesWithWALVersionIfExists(StorageUtils::getRelPropertyListsFName(
                directory, relTableSchema->tableID, direction, propertyID, DBFileType::ORIGINAL));
        }
    }
}

void WALReplayerUtils::replaceListsHeadersFilesWithVersionFromWALIfExists(
    std::unordered_set<RelTableSchema*> relTableSchemas, table_id_t boundTableID,
    const std::string& directory) {
    for (auto relTableSchema : relTableSchemas) {
        for (auto direction : REL_DIRECTIONS) {
            if (!relTableSchema->isSingleMultiplicityInDirection(direction)) {
                auto listsHeadersFileName =
                    StorageUtils::getListHeadersFName(StorageUtils::getAdjListsFName(
                        directory, relTableSchema->tableID, direction, DBFileType::ORIGINAL));
                auto walListsHeadersFileName =
                    StorageUtils::appendWALFileSuffix(listsHeadersFileName);
                FileUtils::renameFileIfExists(walListsHeadersFileName, listsHeadersFileName);
            }
        }
    }
}

void WALReplayerUtils::initLargeListPageListsAndSaveToFile(InMemLists* inMemLists) {
    inMemLists->getListsMetadataBuilder()->initLargeListPageLists(0 /* largeListIdx */);
    inMemLists->saveToFile();
}

void WALReplayerUtils::createEmptyDBFilesForRelProperties(RelTableSchema* relTableSchema,
    const std::string& directory, RelDirection relDirection, uint32_t numNodes,
    bool isForRelPropertyColumn) {
    for (auto& property : relTableSchema->properties) {
        if (isForRelPropertyColumn) {
            auto fName = StorageUtils::getRelPropertyColumnFName(directory, relTableSchema->tableID,
                relDirection, property.propertyID, DBFileType::ORIGINAL);
            InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numNodes)
                ->saveToFile();
        } else {
            auto fName = StorageUtils::getRelPropertyListsFName(directory, relTableSchema->tableID,
                relDirection, property.propertyID, DBFileType::ORIGINAL);
            auto inMemPropertyList =
                InMemListsFactory::getInMemPropertyLists(fName, property.dataType, numNodes);
            initLargeListPageListsAndSaveToFile(inMemPropertyList.get());
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForColumns(
    const std::map<table_id_t, uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
    const std::string& directory, RelTableSchema* relTableSchema) {
    auto boundTableID = relTableSchema->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) == INVALID_NODE_OFFSET ?
                        0 :
                        maxNodeOffsetsPerTable.at(boundTableID) + 1;
    make_unique<InMemAdjColumn>(StorageUtils::getAdjColumnFName(directory, relTableSchema->tableID,
                                    relDirection, DBFileType::ORIGINAL),
        numNodes)
        ->saveToFile();
    createEmptyDBFilesForRelProperties(
        relTableSchema, directory, relDirection, numNodes, true /* isForRelPropertyColumn */);
}

void WALReplayerUtils::createEmptyDBFilesForLists(
    const std::map<table_id_t, uint64_t>& maxNodeOffsetsPerTable, RelDirection relDirection,
    const std::string& directory, RelTableSchema* relTableSchema) {
    auto boundTableID = relTableSchema->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) == INVALID_NODE_OFFSET ?
                        0 :
                        maxNodeOffsetsPerTable.at(boundTableID) + 1;
    auto adjLists =
        make_unique<InMemAdjLists>(StorageUtils::getAdjListsFName(directory,
                                       relTableSchema->tableID, relDirection, DBFileType::ORIGINAL),
            numNodes);
    initLargeListPageListsAndSaveToFile(adjLists.get());
    createEmptyDBFilesForRelProperties(
        relTableSchema, directory, relDirection, numNodes, false /* isForRelPropertyColumn */);
}

void WALReplayerUtils::replaceOriginalColumnFilesWithWALVersionIfExists(
    const std::string& originalColFileName) {
    auto walColFileName = StorageUtils::appendWALFileSuffix(originalColFileName);
    FileUtils::renameFileIfExists(walColFileName, originalColFileName);
    // We also check if there is a WAL version of the overflow file for the column and if so
    // replace the original version.
    FileUtils::renameFileIfExists(StorageUtils::getOverflowFileName(walColFileName),
        StorageUtils::getOverflowFileName(originalColFileName));
}

void WALReplayerUtils::replaceOriginalListFilesWithWALVersionIfExists(
    const std::string& originalListFileName) {
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

void WALReplayerUtils::removeColumnFilesIfExists(const std::string& fileName) {
    FileUtils::removeFileIfExists(fileName);
    FileUtils::removeFileIfExists(StorageUtils::getOverflowFileName(fileName));
}

void WALReplayerUtils::removeListFilesIfExists(const std::string& fileName) {
    FileUtils::removeFileIfExists(fileName);
    FileUtils::removeFileIfExists(StorageUtils::getListMetadataFName(fileName));
    FileUtils::removeFileIfExists(StorageUtils::getOverflowFileName(fileName));
    FileUtils::removeFileIfExists(StorageUtils::getListHeadersFName(fileName));
}

void WALReplayerUtils::fileOperationOnNodeFiles(NodeTableSchema* nodeTableSchema,
    const std::string& directory, std::function<void(std::string fileName)> columnFileOperation,
    std::function<void(std::string fileName)> listFileOperation) {
    for (auto& property : nodeTableSchema->properties) {
        columnFileOperation(StorageUtils::getNodePropertyColumnFName(
            directory, nodeTableSchema->tableID, property.propertyID, DBFileType::ORIGINAL));
    }
    columnFileOperation(
        StorageUtils::getNodeIndexFName(directory, nodeTableSchema->tableID, DBFileType::ORIGINAL));
}

void WALReplayerUtils::fileOperationOnRelFiles(RelTableSchema* relTableSchema,
    const std::string& directory, std::function<void(std::string fileName)> columnFileOperation,
    std::function<void(std::string fileName)> listFileOperation) {
    for (auto relDirection : REL_DIRECTIONS) {
        auto boundTableID = relTableSchema->getBoundTableID(relDirection);
        auto isColumnProperty = relTableSchema->isSingleMultiplicityInDirection(relDirection);
        if (isColumnProperty) {
            columnFileOperation(StorageUtils::getAdjColumnFName(
                directory, relTableSchema->tableID, relDirection, DBFileType::ORIGINAL));
            fileOperationOnRelPropertyFiles(relTableSchema, boundTableID, directory, relDirection,
                isColumnProperty, columnFileOperation, listFileOperation);

        } else {
            listFileOperation(StorageUtils::getAdjListsFName(
                directory, relTableSchema->tableID, relDirection, DBFileType::ORIGINAL));
            fileOperationOnRelPropertyFiles(relTableSchema, boundTableID, directory, relDirection,
                isColumnProperty, columnFileOperation, listFileOperation);
        }
    }
}

void WALReplayerUtils::fileOperationOnRelPropertyFiles(RelTableSchema* tableSchema,
    table_id_t nodeTableID, const std::string& directory, RelDirection relDirection,
    bool isColumnProperty, std::function<void(std::string fileName)> columnFileOperation,
    std::function<void(std::string fileName)> listFileOperation) {
    for (auto& property : tableSchema->properties) {
        if (isColumnProperty) {
            columnFileOperation(StorageUtils::getRelPropertyColumnFName(directory,
                tableSchema->tableID, relDirection, property.propertyID, DBFileType::ORIGINAL));
        } else {
            listFileOperation(StorageUtils::getRelPropertyListsFName(directory,
                tableSchema->tableID, relDirection, property.propertyID, DBFileType::ORIGINAL));
        }
    }
}

} // namespace storage
} // namespace kuzu
