#include "storage/wal_replayer_utils.h"

#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void WALReplayerUtils::removeDBFilesForRelProperty(
    const std::string& directory, RelTableSchema* relTableSchema, property_id_t propertyID) {
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
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
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
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
        if (property.dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            continue;
        }
        auto fName = StorageUtils::getNodePropertyColumnFName(
            directory, nodeTableSchema->tableID, property.propertyID, DBFileType::ORIGINAL);
        std::make_unique<InMemColumn>(fName, property.dataType)->saveToFile();
    }
    switch (nodeTableSchema->getPrimaryKey().dataType.getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        auto pkIndex = make_unique<HashIndexBuilder<int64_t>>(
            StorageUtils::getNodeIndexFName(
                directory, nodeTableSchema->tableID, DBFileType::ORIGINAL),
            nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(0 /* numNodes */);
        pkIndex->flush();
    } break;
    case LogicalTypeID::STRING: {
        auto pkIndex = make_unique<HashIndexBuilder<ku_string_t>>(
            StorageUtils::getNodeIndexFName(
                directory, nodeTableSchema->tableID, DBFileType::ORIGINAL),
            nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(0 /* numNodes */);
        pkIndex->flush();
    } break;
    case LogicalTypeID::SERIAL: {
        // DO NOTHING.
    } break;
    default: {
        throw NotImplementedException("Only INT64, STRING and SERIAL primary keys are supported");
    }
    }
}

void WALReplayerUtils::renameDBFilesForRelProperty(const std::string& directory,
    kuzu::catalog::RelTableSchema* relTableSchema, kuzu::common::property_id_t propertyID) {
    for (auto direction : RelDataDirectionUtils::getRelDataDirections()) {
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

void WALReplayerUtils::createEmptyDBFilesForRelProperties(RelTableSchema* relTableSchema,
    const std::string& directory, RelDataDirection relDirection, uint32_t numNodes,
    bool isForRelPropertyColumn) {
    for (auto& property : relTableSchema->properties) {
        if (isForRelPropertyColumn) {
            auto fName = StorageUtils::getRelPropertyColumnFName(directory, relTableSchema->tableID,
                relDirection, property.propertyID, DBFileType::ORIGINAL);
            std::make_unique<InMemColumn>(fName, property.dataType)->saveToFile();
        } else {
            auto fName = StorageUtils::getRelPropertyListsFName(directory, relTableSchema->tableID,
                relDirection, property.propertyID, DBFileType::ORIGINAL);
            InMemListsFactory::getInMemPropertyLists(
                fName, property.dataType, numNodes, nullptr /* copyDescription */)
                ->saveToFile();
        }
    }
}

void WALReplayerUtils::createEmptyDBFilesForColumns(
    const std::map<table_id_t, uint64_t>& maxNodeOffsetsPerTable, RelDataDirection relDirection,
    const std::string& directory, RelTableSchema* relTableSchema) {
    auto boundTableID = relTableSchema->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) == INVALID_OFFSET ?
                        0 :
                        maxNodeOffsetsPerTable.at(boundTableID) + 1;
    make_unique<InMemColumn>(StorageUtils::getAdjColumnFName(directory, relTableSchema->tableID,
                                 relDirection, DBFileType::ORIGINAL),
        LogicalType(LogicalTypeID::INTERNAL_ID))
        ->saveToFile();
    createEmptyDBFilesForRelProperties(
        relTableSchema, directory, relDirection, numNodes, true /* isForRelPropertyColumn */);
}

void WALReplayerUtils::createEmptyDBFilesForLists(
    const std::map<table_id_t, uint64_t>& maxNodeOffsetsPerTable, RelDataDirection relDirection,
    const std::string& directory, RelTableSchema* relTableSchema) {
    auto boundTableID = relTableSchema->getBoundTableID(relDirection);
    auto numNodes = maxNodeOffsetsPerTable.at(boundTableID) == INVALID_OFFSET ?
                        0 :
                        maxNodeOffsetsPerTable.at(boundTableID) + 1;
    auto adjLists =
        make_unique<InMemAdjLists>(StorageUtils::getAdjListsFName(directory,
                                       relTableSchema->tableID, relDirection, DBFileType::ORIGINAL),
            numNodes);
    adjLists->saveToFile();
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
    FileUtils::renameFileIfExists(StorageUtils::getPropertyNullFName(walColFileName),
        StorageUtils::getPropertyNullFName(originalColFileName));
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
        auto columnFName = StorageUtils::getNodePropertyColumnFName(
            directory, nodeTableSchema->tableID, property.propertyID, DBFileType::ORIGINAL);
        fileOperationOnNodePropertyFile(columnFName, property.dataType, columnFileOperation);
    }
    columnFileOperation(
        StorageUtils::getNodeIndexFName(directory, nodeTableSchema->tableID, DBFileType::ORIGINAL));
}

void WALReplayerUtils::fileOperationOnRelFiles(RelTableSchema* relTableSchema,
    const std::string& directory, std::function<void(std::string fileName)> columnFileOperation,
    std::function<void(std::string fileName)> listFileOperation) {
    for (auto relDirection : RelDataDirectionUtils::getRelDataDirections()) {
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
    table_id_t nodeTableID, const std::string& directory, RelDataDirection relDirection,
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

void WALReplayerUtils::fileOperationOnNodePropertyFile(const std::string& propertyBaseFileName,
    common::LogicalType& propertyType,
    std::function<void(std::string fileName)> columnFileOperation) {
    if (propertyType.getLogicalTypeID() == common::LogicalTypeID::STRUCT) {
        auto fieldTypes = common::StructType::getFieldTypes(&propertyType);
        for (auto i = 0u; i < fieldTypes.size(); i++) {
            fileOperationOnNodePropertyFile(
                StorageUtils::appendStructFieldName(propertyBaseFileName, i), *fieldTypes[i],
                columnFileOperation);
        }
    } else {
        columnFileOperation(propertyBaseFileName);
    }
}

} // namespace storage
} // namespace kuzu
