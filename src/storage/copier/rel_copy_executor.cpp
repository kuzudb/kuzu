#include "storage/copier/rel_copy_executor.h"

#include "common/string_utils.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

RelCopyExecutor::RelCopyExecutor(CopyDescription& copyDescription, WAL* wal,
    TaskScheduler& taskScheduler, Catalog& catalog, NodesStore& nodesStore, RelTable* table,
    RelsStatistics* relsStatistics)
    : copyDescription{copyDescription}, wal{wal}, outputDirectory{std::move(wal->getDirectory())},
      taskScheduler{taskScheduler}, catalog{catalog},
      tableSchema{catalog.getReadOnlyVersion()->getRelTableSchema(table->getRelTableID())},
      numTuples{0}, nodesStore{nodesStore}, table{table}, relsStatistics{relsStatistics} {
    // Initialize rel data.
    fwdRelData = initializeDirectedInMemRelData(FWD);
    bwdRelData = initializeDirectedInMemRelData(BWD);
    pkIndexes.resize(2);
    pkIndexes[0] = nodesStore.getPKIndex(tableSchema->getBoundTableID(FWD));
    pkIndexes[1] = nodesStore.getPKIndex(tableSchema->getBoundTableID(BWD));
}

std::unique_ptr<DirectedInMemRelData> RelCopyExecutor::initializeDirectedInMemRelData(
    RelDataDirection direction) {
    auto directedInMemRelData = std::make_unique<DirectedInMemRelData>();
    auto relSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto boundTableID = reinterpret_cast<RelTableSchema*>(tableSchema)->getBoundTableID(direction);
    auto numNodes =
        nodesStore.getNodesStatisticsAndDeletedIDs().getMaxNodeOffsetPerTable().at(boundTableID) +
        1;
    if (relSchema->isSingleMultiplicityInDirection(direction)) {
        // columns.
        auto relColumns = std::make_unique<DirectedInMemRelColumns>();
        relColumns->adjColumn = std::make_unique<InMemColumn>(
            StorageUtils::getAdjColumnFName(
                outputDirectory, tableSchema->tableID, direction, DBFileType::ORIGINAL),
            LogicalType(LogicalTypeID::INTERNAL_ID));
        relColumns->adjColumnChunk =
            relColumns->adjColumn->getInMemColumnChunk(0, numNodes - 1, &copyDescription);
        for (auto i = 0u; i < tableSchema->getNumProperties(); ++i) {
            auto propertyID = tableSchema->properties[i].propertyID;
            auto propertyDataType = tableSchema->properties[i].dataType;
            auto fName = StorageUtils::getRelPropertyColumnFName(
                outputDirectory, tableSchema->tableID, direction, propertyID, DBFileType::ORIGINAL);
            relColumns->propertyColumns.emplace(
                propertyID, std::make_unique<InMemColumn>(fName, propertyDataType));
            relColumns->propertyColumnChunks.emplace(
                propertyID, relColumns->propertyColumns.at(propertyID)
                                ->getInMemColumnChunk(0, numNodes - 1, &copyDescription));
        }
        directedInMemRelData->setColumns(std::move(relColumns));
    } else {
        // lists.
        auto relLists = std::make_unique<DirectedInMemRelLists>();
        relLists->adjList = std::make_unique<InMemAdjLists>(
            StorageUtils::getAdjListsFName(
                outputDirectory, tableSchema->tableID, direction, DBFileType::ORIGINAL),
            numNodes);
        relLists->relListsSizes = std::make_unique<atomic_uint64_vec_t>(numNodes);
        for (auto i = 0u; i < tableSchema->getNumProperties(); ++i) {
            auto propertyID = tableSchema->properties[i].propertyID;
            auto propertyDataType = tableSchema->properties[i].dataType;
            auto fName = StorageUtils::getRelPropertyListsFName(
                outputDirectory, tableSchema->tableID, direction, propertyID, DBFileType::ORIGINAL);
            relLists->propertyLists.emplace(propertyID,
                InMemListsFactory::getInMemPropertyLists(fName, propertyDataType, numNodes,
                    &copyDescription, relLists->adjList->getListHeadersBuilder()));
        }
        directedInMemRelData->setRelLists(std::move(relLists));
    }
    return directedInMemRelData;
}

offset_t RelCopyExecutor::copy(processor::ExecutionContext* executionContext) {
    wal->logCopyRelRecord(table->getRelTableID());
    // We assume that COPY is a single-statement transaction, thus COPY rel is the only wal record.
    wal->flushAllPages();
    countRelListsSizeAndPopulateColumns(executionContext);
    if (!tableSchema->isSingleMultiplicityInDirection(FWD) ||
        !tableSchema->isSingleMultiplicityInDirection(BWD)) {
        populateRelLists(executionContext);
    }
    relsStatistics->setNumTuplesForTable(tableSchema->tableID, numTuples);
    return numTuples;
}

void RelCopyExecutor::countRelListsSizeAndPopulateColumns(
    processor::ExecutionContext* executionContext) {
    auto relCopier = createRelCopier(RelCopierType::REL_COLUMN_COPIER_AND_LIST_COUNTER);
    auto sharedState = relCopier->getSharedState();
    auto task = std::make_shared<RelCopyTask>(std::move(relCopier), executionContext);
    taskScheduler.scheduleTaskAndWaitOrError(task, executionContext);
    numTuples = sharedState->numTuples;
}

void RelCopyExecutor::populateRelLists(processor::ExecutionContext* executionContext) {
    auto relCopier = createRelCopier(RelCopierType::REL_LIST_COPIER);
    auto task = std::make_shared<RelCopyTask>(std::move(relCopier), executionContext);
    taskScheduler.scheduleTaskAndWaitOrError(task, executionContext);
}

std::unique_ptr<RelCopier> RelCopyExecutor::createRelCopier(RelCopierType relCopierType) {
    std::shared_ptr<CopySharedState> sharedState;
    std::unique_ptr<RelCopier> relCopier;
    std::vector<LogicalType> pkColumnTypes{2};
    pkColumnTypes[0] = catalog.getReadOnlyVersion()
                           ->getNodeTableSchema(tableSchema->getBoundTableID(FWD))
                           ->getPrimaryKey()
                           .dataType;
    pkColumnTypes[1] = catalog.getReadOnlyVersion()
                           ->getNodeTableSchema(tableSchema->getBoundTableID(BWD))
                           ->getPrimaryKey()
                           .dataType;
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV: {
        sharedState = std::make_shared<CSVCopySharedState>(copyDescription.filePaths,
            fileBlockInfos, copyDescription.csvReaderConfig.get(), tableSchema);
        switch (relCopierType) {
        case RelCopierType::REL_COLUMN_COPIER_AND_LIST_COUNTER: {
            relCopier =
                std::make_unique<CSVRelListsCounterAndColumnsCopier>(sharedState, copyDescription,
                    tableSchema, fwdRelData.get(), bwdRelData.get(), pkColumnTypes, pkIndexes);
        } break;
        case RelCopierType::REL_LIST_COPIER: {
            relCopier = std::make_unique<CSVRelListsCopier>(std::move(sharedState), copyDescription,
                tableSchema, fwdRelData.get(), bwdRelData.get(), pkColumnTypes, pkIndexes);
        } break;
        default: {
            throw NotImplementedException(
                "Unsupported RelCopierType in RelCopyExecutor::createRelCopier.");
        }
        }
    } break;
    case CopyDescription::FileType::PARQUET: {
        TableCopyUtils::countNumLines(copyDescription, tableSchema, fileBlockInfos);
        sharedState = std::make_shared<CopySharedState>(copyDescription.filePaths, fileBlockInfos);
        switch (relCopierType) {
        case RelCopierType::REL_COLUMN_COPIER_AND_LIST_COUNTER: {
            relCopier = std::make_unique<ParquetRelListsCounterAndColumnsCopier>(sharedState,
                copyDescription, tableSchema, fwdRelData.get(), bwdRelData.get(), pkColumnTypes,
                pkIndexes);
        } break;
        case RelCopierType::REL_LIST_COPIER: {
            relCopier =
                std::make_unique<ParquetRelListsCopier>(std::move(sharedState), copyDescription,
                    tableSchema, fwdRelData.get(), bwdRelData.get(), pkColumnTypes, pkIndexes);
        } break;
        default: {
            throw NotImplementedException(
                "Unsupported RelCopierType in RelCopyExecutor::createRelCopier.");
        }
        }
    } break;
    default: {
        throw NotImplementedException(StringUtils::string_format(
            "Unsupported file type {} in RelCopyExecutor::createRelCopier.",
            CopyDescription::getFileTypeName(copyDescription.fileType)));
    }
    }
    return relCopier;
}

} // namespace storage
} // namespace kuzu
