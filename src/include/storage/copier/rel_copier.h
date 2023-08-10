#pragma once

#include "storage/copier/read_file_state.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
#include "storage/index/hash_index.h"
#include "storage/store/nodes_store.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace storage {

class DirectedInMemRelData;

class RelCopier {
public:
    RelCopier(std::shared_ptr<ReadFileSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes)
        : sharedState{std::move(sharedState)}, copyDesc{copyDesc}, schema{schema},
          fwdRelData{fwdRelData}, bwdRelData{bwdRelData}, numRows{0}, pkIndexes{
                                                                          std::move(pkIndexes)} {
        fwdCopyStates.resize(schema->getNumProperties());
        for (auto i = 0u; i < schema->getNumProperties(); i++) {
            fwdCopyStates[i] =
                std::make_unique<PropertyCopyState>(*schema->properties[i]->getDataType());
        }
        bwdCopyStates.resize(schema->getNumProperties());
        for (auto i = 0u; i < schema->getNumProperties(); i++) {
            bwdCopyStates[i] =
                std::make_unique<PropertyCopyState>(*schema->properties[i]->getDataType());
        }
    }

    virtual ~RelCopier() = default;

    void execute(processor::ExecutionContext* executionContext);

    virtual std::unique_ptr<RelCopier> clone() const = 0;
    virtual void finalize() = 0;

    inline std::shared_ptr<ReadFileSharedState> getSharedState() const { return sharedState; }

protected:
    virtual void executeInternal(std::unique_ptr<ReadFileMorsel> morsel) {
        throw common::CopyException("RelCopier::executeInternal not implemented");
    }

    static void indexLookup(arrow::Array* pkArray, const common::LogicalType& pkColumnType,
        PrimaryKeyIndex* pkIndex, common::offset_t* offsets, const std::string& filePath,
        common::row_idx_t startRowIdxInFile);

    void copyRelColumnsOrCountRelListsSize(common::row_idx_t rowIdx,
        arrow::RecordBatch* recordBatch, common::RelDataDirection direction,
        const std::vector<std::unique_ptr<arrow::Array>>& pkOffsets);

    void copyRelColumns(common::row_idx_t rowIdx, arrow::RecordBatch* recordBatch,
        common::RelDataDirection direction,
        const std::vector<std::unique_ptr<arrow::Array>>& pkOffsets);
    void countRelListsSize(common::RelDataDirection direction,
        const std::vector<std::unique_ptr<arrow::Array>>& pkOffsets);
    void copyRelLists(common::row_idx_t rowIdx, arrow::RecordBatch* recordBatch,
        common::RelDataDirection direction,
        const std::vector<std::unique_ptr<arrow::Array>>& pkOffsets);
    void checkViolationOfRelColumn(
        common::RelDataDirection direction, arrow::Array* boundNodeOffsets);

    static std::unique_ptr<arrow::PrimitiveArray> createArrowPrimitiveArray(
        const std::shared_ptr<arrow::DataType>& type, const uint8_t* data, uint64_t length);

protected:
    std::shared_ptr<ReadFileSharedState> sharedState;
    common::CopyDescription copyDesc;
    catalog::RelTableSchema* schema;
    DirectedInMemRelData* fwdRelData;
    DirectedInMemRelData* bwdRelData;
    common::row_idx_t numRows;
    std::vector<PrimaryKeyIndex*> pkIndexes;
    std::vector<std::unique_ptr<PropertyCopyState>> fwdCopyStates;
    std::vector<std::unique_ptr<PropertyCopyState>> bwdCopyStates;
};

class RelListsCounterAndColumnCopier : public RelCopier {
protected:
    RelListsCounterAndColumnCopier(std::shared_ptr<ReadFileSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes)
        : RelCopier{std::move(sharedState), copyDesc, schema, fwdRelData, bwdRelData,
              std::move(pkIndexes)} {}

    void finalize() override;

    static void buildRelListsHeaders(
        ListHeadersBuilder* relListHeadersBuilder, const atomic_uint64_vec_t& relListsSizes);
    static void buildRelListsMetadata(DirectedInMemRelData* directedInMemRelData);
    static void buildRelListsMetadata(InMemLists* relLists, ListHeadersBuilder* relListHeaders);
    static void flushRelColumns(DirectedInMemRelData* directedInMemRelData);
};

class ParquetRelListsCounterAndColumnsCopier : public RelListsCounterAndColumnCopier {
public:
    ParquetRelListsCounterAndColumnsCopier(std::shared_ptr<ReadFileSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes)
        : RelListsCounterAndColumnCopier{std::move(sharedState), copyDesc, schema, fwdRelData,
              bwdRelData, std::move(pkIndexes)} {}

    std::unique_ptr<RelCopier> clone() const final {
        return std::make_unique<ParquetRelListsCounterAndColumnsCopier>(
            sharedState, copyDesc, schema, fwdRelData, bwdRelData, pkIndexes);
    }

private:
    void executeInternal(std::unique_ptr<ReadFileMorsel> morsel) final;

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

class CSVRelListsCounterAndColumnsCopier : public RelListsCounterAndColumnCopier {
public:
    CSVRelListsCounterAndColumnsCopier(std::shared_ptr<ReadFileSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes)
        : RelListsCounterAndColumnCopier{std::move(sharedState), copyDesc, schema, fwdRelData,
              bwdRelData, std::move(pkIndexes)} {}

    std::unique_ptr<RelCopier> clone() const final {
        return std::make_unique<CSVRelListsCounterAndColumnsCopier>(
            sharedState, copyDesc, schema, fwdRelData, bwdRelData, pkIndexes);
    }

private:
    void executeInternal(std::unique_ptr<ReadFileMorsel> morsel) final;
};

class RelListsCopier : public RelCopier {
protected:
    RelListsCopier(std::shared_ptr<ReadFileSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes)
        : RelCopier{std::move(sharedState), copyDesc, schema, fwdRelData, bwdRelData,
              std::move(pkIndexes)} {}

private:
    void finalize() final;
};

class ParquetRelListsCopier : public RelListsCopier {
public:
    ParquetRelListsCopier(std::shared_ptr<ReadFileSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes)
        : RelListsCopier{std::move(sharedState), copyDesc, schema, fwdRelData, bwdRelData,
              std::move(pkIndexes)} {}

    std::unique_ptr<RelCopier> clone() const final {
        return std::make_unique<ParquetRelListsCopier>(
            sharedState, copyDesc, schema, fwdRelData, bwdRelData, pkIndexes);
    }

private:
    void executeInternal(std::unique_ptr<ReadFileMorsel> morsel) final;

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

class CSVRelListsCopier : public RelListsCopier {
public:
    CSVRelListsCopier(std::shared_ptr<ReadFileSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes)
        : RelListsCopier{std::move(sharedState), copyDesc, schema, fwdRelData, bwdRelData,
              std::move(pkIndexes)} {}

    std::unique_ptr<RelCopier> clone() const final {
        return std::make_unique<CSVRelListsCopier>(
            sharedState, copyDesc, schema, fwdRelData, bwdRelData, pkIndexes);
    }

private:
    void executeInternal(std::unique_ptr<ReadFileMorsel> morsel) final;
};

class RelCopyTask : public common::Task {
public:
    RelCopyTask(std::unique_ptr<RelCopier> relCopier, processor::ExecutionContext* executionContext)
        : Task{executionContext->numThreads}, relCopier{std::move(relCopier)},
          executionContext{executionContext} {};

    void run() final;
    inline void finalizeIfNecessary() final { relCopier->finalize(); }

private:
    std::mutex mtx;
    std::unique_ptr<RelCopier> relCopier;
    processor::ExecutionContext* executionContext;
};

} // namespace storage
} // namespace kuzu
