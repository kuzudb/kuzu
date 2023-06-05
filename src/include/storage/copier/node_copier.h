#pragma once

#include "storage/copier/npy_reader.h"
#include "storage/copier/table_copy_utils.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/node_table.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

namespace kuzu {
namespace storage {

class NodeCopier {
public:
    NodeCopier(const std::string& directory, std::shared_ptr<CopySharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::TableSchema* schema,
        common::tuple_idx_t numTuples, common::column_id_t columnToCopy);

    // For clone.
    NodeCopier(std::shared_ptr<CopySharedState> sharedState,
        std::shared_ptr<PrimaryKeyIndexBuilder> pkIndex, const common::CopyDescription& copyDesc,
        common::property_id_t pkColumnID, std::vector<std::shared_ptr<InMemColumn>> columns,
        std::vector<catalog::Property> properties, common::column_id_t columnToCopy)
        : sharedState{std::move(sharedState)}, pkIndex{std::move(pkIndex)}, copyDesc{copyDesc},
          pkColumnID{pkColumnID}, columns{std::move(columns)}, properties{std::move(properties)},
          columnToCopy{columnToCopy} {}

    virtual ~NodeCopier() = default;

    void execute(processor::ExecutionContext* executionContext);

    virtual void finalize();

    virtual std::unique_ptr<NodeCopier> clone() const {
        return std::make_unique<NodeCopier>(
            sharedState, pkIndex, copyDesc, pkColumnID, columns, properties, columnToCopy);
    }

protected:
    virtual void executeInternal(std::unique_ptr<CopyMorsel> morsel) {
        throw common::CopyException("NodeCopier::executeInternal not implemented");
    }

    void populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
        common::offset_t startOffset, uint64_t numValues);

    void flushChunksAndPopulatePKIndex(
        const std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks,
        common::offset_t startNodeOffset, common::offset_t endNodeOffset);

    template<typename T, typename... Args>
    void appendToPKIndex(
        InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues, Args... args) {
        throw common::CopyException("appendToPKIndex not implemented");
    }

private:
    void initializeIndex(
        const std::string& directory, catalog::TableSchema* schema, common::tuple_idx_t numTuples);

protected:
    std::shared_ptr<CopySharedState> sharedState;
    std::shared_ptr<PrimaryKeyIndexBuilder> pkIndex;
    common::CopyDescription copyDesc;
    // The properties to be copied into. Each property corresponds to a column.
    std::vector<catalog::Property> properties;
    std::vector<std::shared_ptr<InMemColumn>> columns;
    common::column_id_t pkColumnID;
    common::column_id_t columnToCopy;
};

template<>
void NodeCopier::appendToPKIndex<int64_t>(
    kuzu::storage::InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues);
template<>
void NodeCopier::appendToPKIndex<common::ku_string_t, storage::InMemOverflowFile*>(
    kuzu::storage::InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues,
    storage::InMemOverflowFile* overflowFile);

class CSVNodeCopier : public NodeCopier {
public:
    CSVNodeCopier(const std::string& directory, std::shared_ptr<CopySharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::TableSchema* schema,
        common::tuple_idx_t numTuples, common::column_id_t columnToCopy)
        : NodeCopier{directory, std::move(sharedState), copyDesc, schema, numTuples, columnToCopy} {
    }

    // For clone.
    CSVNodeCopier(std::shared_ptr<CopySharedState> sharedState,
        std::shared_ptr<PrimaryKeyIndexBuilder> pkIndex, const common::CopyDescription& copyDesc,
        common::column_id_t pkColumnID, std::vector<std::shared_ptr<InMemColumn>> columns,
        std::vector<catalog::Property> properties, common::column_id_t columnToCopy)
        : NodeCopier{std::move(sharedState), std::move(pkIndex), copyDesc, pkColumnID,
              std::move(columns), std::move(properties), columnToCopy} {}

    inline std::unique_ptr<NodeCopier> clone() const override {
        return std::make_unique<CSVNodeCopier>(
            sharedState, pkIndex, copyDesc, pkColumnID, columns, properties, columnToCopy);
    }

protected:
    void executeInternal(std::unique_ptr<CopyMorsel> morsel) override;
};

class ParquetNodeCopier : public NodeCopier {
public:
    ParquetNodeCopier(const std::string& directory, std::shared_ptr<CopySharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::TableSchema* schema,
        common::tuple_idx_t numTuples, common::column_id_t columnToCopy)
        : NodeCopier{directory, std::move(sharedState), copyDesc, schema, numTuples, columnToCopy} {
    }

    // Clone.
    ParquetNodeCopier(std::shared_ptr<CopySharedState> sharedState,
        std::shared_ptr<PrimaryKeyIndexBuilder> pkIndex, const common::CopyDescription& copyDesc,
        common::column_id_t pkColumnID, std::vector<std::shared_ptr<InMemColumn>> columns,
        std::vector<catalog::Property> properties, common::column_id_t columnToCopy)
        : NodeCopier{std::move(sharedState), std::move(pkIndex), copyDesc, pkColumnID,
              std::move(columns), std::move(properties), columnToCopy} {}

    inline std::unique_ptr<NodeCopier> clone() const override {
        return std::make_unique<ParquetNodeCopier>(
            sharedState, pkIndex, copyDesc, pkColumnID, columns, properties, columnToCopy);
    }

protected:
    void executeInternal(std::unique_ptr<CopyMorsel> morsel) override;

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

class NPYNodeCopier : public NodeCopier {
public:
    NPYNodeCopier(const std::string& directory, std::shared_ptr<CopySharedState> sharedState,
        const common::CopyDescription& copyDesc, catalog::TableSchema* schema,
        common::tuple_idx_t numTuples, common::column_id_t columnToCopy)
        : NodeCopier{directory, std::move(sharedState), copyDesc, schema, numTuples, columnToCopy} {
    }

    // Clone.
    NPYNodeCopier(std::shared_ptr<CopySharedState> sharedState,
        std::shared_ptr<PrimaryKeyIndexBuilder> pkIndex, const common::CopyDescription& copyDesc,
        common::column_id_t pkColumnID, std::vector<std::shared_ptr<InMemColumn>> columns,
        std::vector<catalog::Property> properties, common::column_id_t columnToCopy)
        : NodeCopier{std::move(sharedState), std::move(pkIndex), copyDesc, pkColumnID,
              std::move(columns), std::move(properties), columnToCopy} {}

    inline std::unique_ptr<NodeCopier> clone() const override {
        return std::make_unique<NPYNodeCopier>(
            sharedState, pkIndex, copyDesc, pkColumnID, columns, properties, columnToCopy);
    }

protected:
    void executeInternal(std::unique_ptr<CopyMorsel> morsel) override;

private:
    std::unique_ptr<NpyReader> reader;
};

// Note: This is a temporary class used specially in the node copier. NodeCopyTask mimics the
// behaviour of ProcessorTask. Eventually, we shouldn't have both CopyTask and NodeCopyTask.
class NodeCopyTask : public common::Task {
public:
    NodeCopyTask(
        std::unique_ptr<NodeCopier> nodeCopier, processor::ExecutionContext* executionContext)
        : Task{executionContext->numThreads}, nodeCopier{std::move(nodeCopier)},
          executionContext{executionContext} {};

    void run() override;
    inline void finalizeIfNecessary() override { nodeCopier->finalize(); }

private:
    std::mutex mtx;
    std::unique_ptr<NodeCopier> nodeCopier;
    processor::ExecutionContext* executionContext;
};

} // namespace storage
} // namespace kuzu
