#pragma once

#include "parquet/parquet_types.h"
#include "processor/operator/persistent/writer/parquet/parquet_writer.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class CopyToParquetSharedState {
public:
    std::unique_ptr<ParquetWriter> writer;
};

struct CopyToParquetInfo {
    kuzu_parquet::format::CompressionCodec::type codec =
        kuzu_parquet::format::CompressionCodec::SNAPPY;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
    std::vector<std::unique_ptr<common::LogicalType>> types;
    std::vector<std::string> names;
    std::vector<DataPos> dataPoses;
    std::string fileName;

    CopyToParquetInfo(std::unique_ptr<FactorizedTableSchema> tableSchema,
        std::vector<std::unique_ptr<common::LogicalType>> types, std::vector<std::string> names,
        std::vector<DataPos> dataPoses, std::string fileName)
        : tableSchema{std::move(tableSchema)}, types{std::move(types)}, names{std::move(names)},
          dataPoses{std::move(dataPoses)}, fileName{std::move(fileName)} {}

    std::vector<std::unique_ptr<common::LogicalType>> copyTypes();

    std::unique_ptr<CopyToParquetInfo> copy() {
        return std::make_unique<CopyToParquetInfo>(
            tableSchema->copy(), copyTypes(), names, dataPoses, fileName);
    }
};

struct CopyToParquetLocalState {
    std::unique_ptr<FactorizedTable> ft;
    std::vector<common::ValueVector*> vectorsToAppend;
    storage::MemoryManager* mm;

    void init(CopyToParquetInfo* info, storage::MemoryManager* mm, ResultSet* resultSet);

    inline void append() { ft->append(vectorsToAppend); }
};

class CopyToParquet : public Sink {
public:
    CopyToParquet(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<CopyToParquetInfo> info,
        std::shared_ptr<CopyToParquetSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_TO_PARQUET,
              std::move(child), id, paramsString},
          info{std::move(info)}, localState{std::make_unique<CopyToParquetLocalState>()},
          sharedState{std::move(sharedState)} {}

    inline void finalize(ExecutionContext* executionContext) final {
        sharedState->writer->finalize();
    }
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;
    void executeInternal(ExecutionContext* context) final;
    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyToParquet>(resultSetDescriptor->copy(), info->copy(),
            sharedState, children[0]->clone(), id, paramsString);
    }

private:
    void initGlobalStateInternal(ExecutionContext* context) final {
        sharedState->writer = std::make_unique<ParquetWriter>(
            info->fileName, info->copyTypes(), info->names, info->codec, context->memoryManager);
    }

private:
    std::shared_ptr<CopyToParquetSharedState> sharedState;
    std::unique_ptr<CopyToParquetLocalState> localState;
    std::unique_ptr<CopyToParquetInfo> info;
};

} // namespace processor
} // namespace kuzu
