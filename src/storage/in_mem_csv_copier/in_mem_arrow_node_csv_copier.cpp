#include "include/in_mem_arrow_node_csv_copier.h"

#include "include/copy_csv_task.h"

#include "src/storage/storage_structure/include/in_mem_file.h"

#include <iostream>

namespace kuzu {
namespace storage {

    InMemArrowNodeCSVCopier::InMemArrowNodeCSVCopier(CSVDescription &csvDescription, string outputDirectory,
                                                     TaskScheduler &taskScheduler, Catalog &catalog, table_id_t tableID,
                                                     NodesStatisticsAndDeletedIDs *nodesStatisticsAndDeletedIDs)
            : InMemStructuresCSVCopier{csvDescription, move(outputDirectory), taskScheduler, catalog},
              numNodes{UINT64_MAX}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {
        nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(tableID);
    }

    uint64_t InMemArrowNodeCSVCopier::copy() {
        arrow::Status status;
        status = initializeArrowCSV(csvDescription.filePath);
        if (!status.ok()) {
            throw CopyCSVException(status.ToString());
        }

        initializeColumnsAndList();

        switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
            case INT64: {
                status = arrowPopulateColumns<int64_t>();
            }
                break;
            case STRING: {
                status = arrowPopulateColumns<ku_string_t>();
            }
                break;
            default: {
                throw CopyCSVException("Unsupported data type " +
                                       Types::dataTypeToString(nodeTableSchema->getPrimaryKey().dataType) +
                                       " for the ID index.");
            }
        }

        if (!status.ok()) {
            throw CopyCSVException(status.ToString());
        }

        saveToFile();
        nodesStatisticsAndDeletedIDs->setNumTuplesForTable(nodeTableSchema->tableID, numNodes);
        logger->info("Done copying node {} with table {}.", nodeTableSchema->tableName,
                     nodeTableSchema->tableID);
        return numNodes;
    }

    arrow::Status InMemArrowNodeCSVCopier::initializeArrowCSV(const std::string &filePath) {
        shared_ptr<arrow::io::InputStream> arrow_input_stream;
        shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
        ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(filePath));
        ARROW_ASSIGN_OR_RAISE(
                csv_streaming_reader,
                arrow::csv::StreamingReader::Make(
                        arrow::io::default_io_context(),
                        arrow_input_stream,
                        arrow::csv::ReadOptions::Defaults(),
                        arrow::csv::ParseOptions::Defaults(),
                        arrow::csv::ConvertOptions::Defaults()
                        ));


        numBlocks = 0;
        numNodes = 0;
        std::shared_ptr<arrow::RecordBatch> currBatch;

        auto endIt = csv_streaming_reader->end();
        for (auto it = csv_streaming_reader->begin(); it != endIt; ++ it) {
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            ++ numBlocks;
            auto currNumRows = currBatch->num_rows();
            numLinesPerBlock.push_back(currNumRows);
            numNodes += currNumRows;
        }

        return arrow::Status::OK();
    }

    void InMemArrowNodeCSVCopier::initializeColumnsAndList() {
        logger->info("Initializing in memory structured columns.");
        structuredColumns.resize(nodeTableSchema->getNumStructuredProperties());
        for (auto& property : nodeTableSchema->structuredProperties) {
            auto fName = StorageUtils::getNodePropertyColumnFName(outputDirectory,
                                                                  nodeTableSchema->tableID, property.propertyID, DBFileType::WAL_VERSION);
            structuredColumns[property.propertyID] =
                    InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numNodes);
        }
        logger->info("Done initializing in memory structured columns.");
    }

    template<typename T>
    arrow::Status InMemArrowNodeCSVCopier::arrowPopulateColumns() {
        logger->info("Populating structured properties");
        auto pkIndex =
                make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                                                                 nodeTableSchema->tableID,
                                                                                 DBFileType::WAL_VERSION),
                                                 nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(numNodes);
        node_offset_t offsetStart = 0;

        shared_ptr<arrow::io::InputStream> arrow_input_stream;
        shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
        ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(csvDescription.filePath));
        ARROW_ASSIGN_OR_RAISE(
                csv_streaming_reader,
                arrow::csv::StreamingReader::Make(
                        arrow::io::default_io_context(),
                        arrow_input_stream,
                        arrow::csv::ReadOptions::Defaults(),
                        arrow::csv::ParseOptions::Defaults(),
                        arrow::csv::ConvertOptions::Defaults()
                ));

        std::shared_ptr<arrow::RecordBatch> currBatch;

        int blockIdx = 0;
        auto endIt = csv_streaming_reader->end();
        for (auto it = csv_streaming_reader->begin(); it != endIt; ++ it) {
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    arrowPopulateColumnsTask<T>,
                    nodeTableSchema->primaryKeyPropertyIdx,
                    blockIdx, offsetStart, pkIndex.get(), this, currBatch));
            offsetStart += currBatch->num_rows();
            ++ blockIdx;
        }

        taskScheduler.waitAllTasksToCompleteOrError();
        logger->info("Flush the pk index to disk.");
        pkIndex->flush();
        logger->info("Done populating structured properties, constructing the pk index.");

        return arrow::Status::OK();
    }

    template<typename T>
    void InMemArrowNodeCSVCopier::addIDsToIndex(InMemColumn *column, HashIndexBuilder<T> *hashIndex,
                                                node_offset_t startOffset, uint64_t numValues) {
        for (auto i = 0u; i < numValues; i++) {
            auto offset = i + startOffset;
            if constexpr (is_same<T, int64_t>::value) {
                auto key = (int64_t *) column->getElement(offset);
                if (!hashIndex->append(*key, offset)) {
                    throw CopyCSVException("ID value " + to_string(*key) +
                                           " violates the uniqueness constraint for the ID property.");
                }
            } else {
                auto element = (ku_string_t *) column->getElement(offset);
                auto key = column->getInMemOverflowFile()->readString(element);
                if (!hashIndex->append(key.c_str(), offset)) {
                    throw CopyCSVException("ID value  " + key +
                                           " violates the uniqueness constraint for the ID property.");
                }
            }
        }
    }

    template<typename T>
    void InMemArrowNodeCSVCopier::populatePKIndex(InMemColumn *column, HashIndexBuilder<T> *pkIndex,
                                                  node_offset_t startOffset, uint64_t numValues) {
        addIDsToIndex(column, pkIndex, startOffset, numValues);
    }

    template<typename T>
    arrow::Status InMemArrowNodeCSVCopier::arrowPopulateColumnsTask(uint64_t primaryKeyPropertyIdx,
                                                                 uint64_t blockId,
                                                                 uint64_t offsetStart,
                                                                 HashIndexBuilder<T> *pkIndex,
                                                                 InMemArrowNodeCSVCopier *copier,
                                                                 std::shared_ptr<arrow::RecordBatch> currBatch) {
        copier->logger->trace("Start: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumStructuredProperties());
        auto arrow_columns = currBatch->columns();
        // TODO: Consider skip header
//        skipFirstRowIfNecessary(blockId, copier->csvDescription, reader);
        for (auto bufferOffset = 0u; bufferOffset < copier->numLinesPerBlock[blockId]; ++ bufferOffset) {
            putPropsOfLineIntoColumns(copier->structuredColumns,
                                      copier->nodeTableSchema->structuredProperties,
                                      overflowCursors,
                                      arrow_columns,
                                      offsetStart + bufferOffset,
                                      bufferOffset);
        }
        populatePKIndex(copier->structuredColumns[primaryKeyPropertyIdx].get(), pkIndex, offsetStart,
                        copier->numLinesPerBlock[blockId]);
        copier->logger->trace("End: path={0} blkIdx={1}", copier->csvDescription.filePath, blockId);
        return arrow::Status::OK();
    }

    void InMemArrowNodeCSVCopier::putPropsOfLineIntoColumns(
            vector<unique_ptr<InMemColumn>> &structuredColumns,
            const vector<Property> &structuredProperties, vector<PageByteCursor> &overflowCursors,
            std::vector<shared_ptr<arrow::Array>> &arrow_columns,
            uint64_t nodeOffset, uint64_t bufferOffset) {
        for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
            auto column = structuredColumns[columnIdx].get();

            // TODO: To improve efficiency, do not slice here. Slice before this function call

            auto currentTokenArray = arrow_columns[columnIdx]->Slice(bufferOffset);

            switch (column->getDataType().typeID) {
                case INT64: {
                    if (!arrow_columns[columnIdx]->IsNull(bufferOffset)) {
                        auto tokenIntArray = std::static_pointer_cast<arrow::Int64Array>(currentTokenArray);
                        int64_t int64Val = tokenIntArray->Value(0);
                        column->setElement(nodeOffset, reinterpret_cast<uint8_t *>(&int64Val));
                    }
                }
                    break;
                default:
                    break;
            }
        }
    }


    void InMemArrowNodeCSVCopier::saveToFile() {
        logger->debug("Writing node structured columns to disk.");
        assert(!structuredColumns.empty());
        for (auto &column: structuredColumns) {
            taskScheduler.scheduleTask(CopyCSVTaskFactory::createCopyCSVTask(
                    [&](InMemColumn *x) { x->saveToFile(); }, column.get()));
        }
        taskScheduler.waitAllTasksToCompleteOrError();
        logger->debug("Done writing node structured columns to disk.");
    }

} // namespace storage
} // namespace kuzu
