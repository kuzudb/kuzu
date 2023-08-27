#pragma once

#include "storage/copier/reader_state.h"
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
    RelCopier(std::shared_ptr<ReaderSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes, read_rows_func_t readFunc,
        init_reader_data_func_t initFunc)
        : sharedState{std::move(sharedState)}, copyDesc{copyDesc}, schema{schema},
          fwdRelData{fwdRelData}, bwdRelData{bwdRelData}, numRows{0}, pkIndexes{std::move(
                                                                          pkIndexes)},
          readFuncData{nullptr}, readFunc{std::move(readFunc)}, initFunc{std::move(initFunc)} {
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

    inline std::shared_ptr<ReaderSharedState> getSharedState() const { return sharedState; }

protected:
    virtual bool executeInternal() {
        throw common::NotImplementedException("RelCopier::executeInternal not implemented");
    }

    static void indexLookup(arrow::Array* pkArray, const common::LogicalType& pkColumnType,
        PrimaryKeyIndex* pkIndex, common::offset_t* offsets);

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
    std::shared_ptr<ReaderSharedState> sharedState;
    common::CopyDescription copyDesc;
    catalog::RelTableSchema* schema;
    DirectedInMemRelData* fwdRelData;
    DirectedInMemRelData* bwdRelData;
    common::row_idx_t numRows;
    std::vector<PrimaryKeyIndex*> pkIndexes;
    std::vector<std::unique_ptr<PropertyCopyState>> fwdCopyStates;
    std::vector<std::unique_ptr<PropertyCopyState>> bwdCopyStates;

    std::unique_ptr<storage::ReaderFunctionData> readFuncData;
    storage::read_rows_func_t readFunc;
    storage::init_reader_data_func_t initFunc;
};

class RelListsCounterAndColumnCopier : public RelCopier {
protected:
    RelListsCounterAndColumnCopier(std::shared_ptr<ReaderSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes, read_rows_func_t readFunc,
        init_reader_data_func_t initFunc)
        : RelCopier{std::move(sharedState), copyDesc, schema, fwdRelData, bwdRelData,
              std::move(pkIndexes), std::move(readFunc), std::move(initFunc)} {}

    void finalize() override;

    static void buildRelListsHeaders(
        ListHeadersBuilder* relListHeadersBuilder, const atomic_uint64_vec_t& relListsSizes);
    static void buildRelListsMetadata(DirectedInMemRelData* directedInMemRelData);
    static void buildRelListsMetadata(InMemLists* relLists, ListHeadersBuilder* relListHeaders);
    static void flushRelColumns(DirectedInMemRelData* directedInMemRelData);
};

class ParquetRelListsCounterAndColumnsCopier : public RelListsCounterAndColumnCopier {
public:
    ParquetRelListsCounterAndColumnsCopier(std::shared_ptr<ReaderSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes, read_rows_func_t readFunc,
        init_reader_data_func_t initFunc)
        : RelListsCounterAndColumnCopier{std::move(sharedState), copyDesc, schema, fwdRelData,
              bwdRelData, std::move(pkIndexes), std::move(readFunc), std::move(initFunc)} {}

    std::unique_ptr<RelCopier> clone() const final {
        return std::make_unique<ParquetRelListsCounterAndColumnsCopier>(
            sharedState, copyDesc, schema, fwdRelData, bwdRelData, pkIndexes, readFunc, initFunc);
    }

private:
    bool executeInternal() final;
};

class CSVRelListsCounterAndColumnsCopier : public RelListsCounterAndColumnCopier {
public:
    CSVRelListsCounterAndColumnsCopier(std::shared_ptr<ReaderSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes, read_rows_func_t readFunc,
        init_reader_data_func_t initFunc)
        : RelListsCounterAndColumnCopier{std::move(sharedState), copyDesc, schema, fwdRelData,
              bwdRelData, std::move(pkIndexes), std::move(readFunc), std::move(initFunc)} {}

    std::unique_ptr<RelCopier> clone() const final {
        return std::make_unique<CSVRelListsCounterAndColumnsCopier>(
            sharedState, copyDesc, schema, fwdRelData, bwdRelData, pkIndexes, readFunc, initFunc);
    }

private:
    bool executeInternal() final;
};

class RelListsCopier : public RelCopier {
protected:
    RelListsCopier(std::shared_ptr<ReaderSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes, read_rows_func_t readFunc,
        init_reader_data_func_t initFunc)
        : RelCopier{std::move(sharedState), copyDesc, schema, fwdRelData, bwdRelData,
              std::move(pkIndexes), std::move(readFunc), std::move(initFunc)} {}

private:
    void finalize() final;
};

class ParquetRelListsCopier : public RelListsCopier {
public:
    ParquetRelListsCopier(std::shared_ptr<ReaderSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes, read_rows_func_t readFunc,
        init_reader_data_func_t initFunc)
        : RelListsCopier{std::move(sharedState), copyDesc, schema, fwdRelData, bwdRelData,
              std::move(pkIndexes), std::move(readFunc), std::move(initFunc)} {}

    std::unique_ptr<RelCopier> clone() const final {
        return std::make_unique<ParquetRelListsCopier>(
            sharedState, copyDesc, schema, fwdRelData, bwdRelData, pkIndexes, readFunc, initFunc);
    }

private:
    bool executeInternal() final;
};

class CSVRelListsCopier : public RelListsCopier {
public:
    CSVRelListsCopier(std::shared_ptr<ReaderSharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::RelTableSchema* schema,
        DirectedInMemRelData* fwdRelData, DirectedInMemRelData* bwdRelData,
        std::vector<PrimaryKeyIndex*> pkIndexes, read_rows_func_t readFunc,
        init_reader_data_func_t initFunc)
        : RelListsCopier{std::move(sharedState), copyDesc, schema, fwdRelData, bwdRelData,
              std::move(pkIndexes), std::move(readFunc), std::move(initFunc)} {}

    std::unique_ptr<RelCopier> clone() const final {
        return std::make_unique<CSVRelListsCopier>(
            sharedState, copyDesc, schema, fwdRelData, bwdRelData, pkIndexes, readFunc, initFunc);
    }

private:
    bool executeInternal() final;
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
