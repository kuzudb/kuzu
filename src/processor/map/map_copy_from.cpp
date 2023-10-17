#include "binder/copy/bound_copy_from.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "processor/operator/index_lookup.h"
#include "processor/operator/persistent/copy_node.h"
#include "processor/operator/persistent/copy_rdf_resource.h"
#include "processor/operator/persistent/copy_rel.h"
#include "processor/operator/persistent/copy_rel_columns.h"
#include "processor/operator/persistent/copy_rel_lists.h"
#include "processor/operator/persistent/reader.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    switch (copyFrom->getInfo()->tableSchema->getTableType()) {
    case TableType::NODE:
        return mapCopyNodeFrom(logicalOperator);
    case TableType::REL:
        return mapCopyRelFrom(logicalOperator);
        // LCOV_EXCL_START
    default:
        throw NotImplementedException{"PlanMapper::mapCopy"};
    }
    // LCOV_EXCL_STOP
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNodeFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = reinterpret_cast<LogicalCopyFrom*>(logicalOperator);
    auto copyFromInfo = copyFrom->getInfo();
    auto tableSchema = (NodeTableSchema*)copyFromInfo->tableSchema;
    auto nodeTable = storageManager.getNodesStore().getNodeTable(tableSchema->tableID);
    // Map reader.
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    auto reader = reinterpret_cast<Reader*>(prevOperator.get());
    auto readerInfo = reader->getReaderInfo();
    // Populate shared state
    auto sharedState =
        std::make_shared<CopyNodeSharedState>(reader->getSharedState()->getNumRowsRef());
    sharedState->wal = storageManager.getWAL();
    sharedState->table = nodeTable;
    auto properties = tableSchema->getProperties();
    for (auto& property : properties) {
        sharedState->columnTypes.push_back(property->getDataType()->copy());
    }
    auto pk = tableSchema->getPrimaryKey();
    for (auto i = 0u; i < properties.size(); ++i) {
        if (properties[i]->getPropertyID() == pk->getPropertyID()) {
            sharedState->pkColumnIdx = i;
        }
    }
    sharedState->pkType = pk->getDataType()->copy();
    sharedState->fTable = getSingleStringColumnFTable();
    // Populate info
    std::unordered_set<common::table_id_t> connectedRelTableIDSet;
    for (auto tableID : tableSchema->getFwdRelTableIDSet()) {
        connectedRelTableIDSet.insert(tableID);
    }
    for (auto tableID : tableSchema->getBwdRelTableIDSet()) {
        connectedRelTableIDSet.insert(tableID);
    }
    std::vector<storage::RelTable*> connectedRelTables;
    for (auto tableID : connectedRelTableIDSet) {
        connectedRelTables.push_back(storageManager.getRelsStore().getRelTable(tableID));
    }
    auto info = std::make_unique<CopyNodeInfo>(readerInfo->dataColumnsPos, nodeTable,
        tableSchema->tableName, std::move(connectedRelTables), copyFromInfo->containsSerial);

    std::unique_ptr<PhysicalOperator> copyNode;
    auto readerConfig = copyFromInfo->fileScanInfo->readerConfig.get();
    if (readerConfig->fileType == FileType::TURTLE &&
        readerConfig->rdfReaderConfig->mode == RdfReaderMode::RESOURCE) {
        copyNode = std::make_unique<CopyRdfResource>(sharedState, std::move(info),
            std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(prevOperator),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    } else {
        copyNode = std::make_unique<CopyNode>(sharedState, std::move(info),
            std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(prevOperator),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    }
    auto outputExpressions = binder::expression_vector{copyFrom->getOutputExpression()->copy()};
    return createFactorizedTableScanAligned(outputExpressions, copyFrom->getSchema(),
        sharedState->fTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(copyNode));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createCopyRelColumnsOrLists(
    std::shared_ptr<CopyRelSharedState> sharedState, LogicalCopyFrom* copyFrom, bool isColumns,
    std::unique_ptr<PhysicalOperator> copyRelColumns) {
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    Reader* reader;
    if (prevOperator->getOperatorType() == PhysicalOperatorType::READER) {
        reader = reinterpret_cast<Reader*>(prevOperator.get());
    } else {
        assert(prevOperator->getChild(0)->getOperatorType() == PhysicalOperatorType::READER);
        reader = reinterpret_cast<Reader*>(prevOperator->getChild(0));
    }

    auto tableSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    auto internalIDDataPos =
        DataPos{outFSchema->getExpressionPos(*copyFromInfo->fileScanInfo->internalID)};
    DataPos srcOffsetPos;
    DataPos dstOffsetPos;
    std::vector<DataPos> dataColumnPositions;
    if (copyFromInfo->fileScanInfo->readerConfig->fileType == FileType::TURTLE) {
        auto extraInfo = reinterpret_cast<ExtraBoundCopyRdfRelInfo*>(copyFromInfo->extraInfo.get());
        srcOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->subjectOffset)};
        dstOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->objectOffset)};
        dataColumnPositions.emplace_back(srcOffsetPos);
        dataColumnPositions.emplace_back(dstOffsetPos);
        dataColumnPositions.emplace_back(outFSchema->getExpressionPos(*extraInfo->predicateOffset));
    } else {
        auto extraInfo = reinterpret_cast<ExtraBoundCopyRelInfo*>(copyFromInfo->extraInfo.get());
        srcOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->srcOffset)};
        dstOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->dstOffset)};
        dataColumnPositions = reader->getReaderInfo()->dataColumnsPos;
    }
    CopyRelInfo copyRelInfo{tableSchema, dataColumnPositions, internalIDDataPos, srcOffsetPos,
        dstOffsetPos, storageManager.getWAL(), copyFromInfo->containsSerial};
    if (isColumns) {
        return std::make_unique<CopyRelColumns>(copyRelInfo, std::move(sharedState),
            std::make_unique<ResultSetDescriptor>(outFSchema), std::move(prevOperator),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    } else {
        return std::make_unique<CopyRelLists>(copyRelInfo, std::move(sharedState),
            std::make_unique<ResultSetDescriptor>(outFSchema), std::move(prevOperator),
            std::move(copyRelColumns), getOperatorID(), copyFrom->getExpressionsForPrinting());
    }
}

static std::unique_ptr<DirectedInMemRelData> initializeDirectedInMemRelData(
    common::RelDataDirection direction, RelTableSchema* schema, NodesStore& nodesStore,
    const std::string& outputDirectory, CSVReaderConfig* csvReaderConfig) {
    auto directedInMemRelData = std::make_unique<DirectedInMemRelData>();
    auto boundTableID = schema->getBoundTableID(direction);
    auto numNodes =
        nodesStore.getNodesStatisticsAndDeletedIDs()->getMaxNodeOffsetPerTable().at(boundTableID) +
        1;
    if (schema->isSingleMultiplicityInDirection(direction)) {
        // columns.
        auto relColumns = std::make_unique<DirectedInMemRelColumns>();
        relColumns->adjColumn =
            std::make_unique<InMemColumn>(StorageUtils::getAdjColumnFName(outputDirectory,
                                              schema->tableID, direction, DBFileType::ORIGINAL),
                LogicalType(LogicalTypeID::INTERNAL_ID));
        relColumns->adjColumnChunk =
            relColumns->adjColumn->createInMemColumnChunk(0, numNodes - 1, csvReaderConfig);
        for (auto i = 0u; i < schema->getNumProperties(); ++i) {
            auto propertyID = schema->properties[i]->getPropertyID();
            auto propertyDataType = schema->properties[i]->getDataType();
            auto fName = StorageUtils::getRelPropertyColumnFName(
                outputDirectory, schema->tableID, direction, propertyID, DBFileType::ORIGINAL);
            relColumns->propertyColumns.emplace(
                propertyID, std::make_unique<InMemColumn>(fName, *propertyDataType));
            relColumns->propertyColumnChunks.emplace(
                propertyID, relColumns->propertyColumns.at(propertyID)
                                ->createInMemColumnChunk(0, numNodes - 1, csvReaderConfig));
        }
        directedInMemRelData->setColumns(std::move(relColumns));
    } else {
        // lists.
        auto relLists = std::make_unique<DirectedInMemRelLists>();
        relLists->adjList =
            std::make_unique<InMemAdjLists>(StorageUtils::getAdjListsFName(outputDirectory,
                                                schema->tableID, direction, DBFileType::ORIGINAL),
                numNodes);
        relLists->relListsSizes = std::make_unique<atomic_uint64_vec_t>(numNodes);
        for (auto i = 0u; i < schema->getNumProperties(); ++i) {
            auto property = schema->getProperty(i);
            auto fName = StorageUtils::getRelPropertyListsFName(outputDirectory, schema->tableID,
                direction, property->getPropertyID(), DBFileType::ORIGINAL);
            relLists->propertyLists.emplace(property->getPropertyID(),
                InMemListsFactory::getInMemPropertyLists(fName, *property->getDataType(), numNodes,
                    csvReaderConfig, relLists->adjList->getListHeadersBuilder()));
        }
        directedInMemRelData->setRelLists(std::move(relLists));
    }
    return directedInMemRelData;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyRelFrom(
    planner::LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto tableSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);

    auto fwdRelData = initializeDirectedInMemRelData(RelDataDirection::FWD, tableSchema,
        storageManager.getNodesStore(), storageManager.getDirectory(),
        copyFromInfo->fileScanInfo->readerConfig->csvReaderConfig.get());
    auto bwdRelData = initializeDirectedInMemRelData(RelDataDirection::BWD, tableSchema,
        storageManager.getNodesStore(), storageManager.getDirectory(),
        copyFromInfo->fileScanInfo->readerConfig->csvReaderConfig.get());
    auto copyRelSharedState = std::make_shared<CopyRelSharedState>(tableSchema->tableID,
        storageManager.getRelsStore().getRelsStatistics(), std::move(fwdRelData),
        std::move(bwdRelData), memoryManager);

    auto copyRelColumns = createCopyRelColumnsOrLists(
        copyRelSharedState, copyFrom, true /* isColumns */, nullptr /* child */);
    auto copyRelLists = createCopyRelColumnsOrLists(
        copyRelSharedState, copyFrom, false /* isColumns */, std::move(copyRelColumns));
    auto outputExpressions = expression_vector{copyFrom->getOutputExpression()->copy()};
    return createFactorizedTableScanAligned(outputExpressions, outFSchema,
        copyRelSharedState->getFTable(), DEFAULT_VECTOR_CAPACITY /* maxMorselSize */,
        std::move(copyRelLists));
}

} // namespace processor
} // namespace kuzu
