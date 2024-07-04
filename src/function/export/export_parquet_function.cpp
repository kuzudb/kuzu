
#include "function/export/export_function.h"
#include "main/client_context.h"
#include "parquet/parquet_types.h"
#include "processor/operator/persistent/writer/parquet/parquet_writer.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace function {

using namespace common;
using namespace processor;

struct ExportParquetBindData final : public ExportFuncBindData {
    kuzu_parquet::format::CompressionCodec::type codec =
        kuzu_parquet::format::CompressionCodec::SNAPPY;

    ExportParquetBindData(std::vector<std::string> names, std::string fileName)
        : ExportFuncBindData{std::move(names), std::move(fileName)} {}

    std::unique_ptr<ExportFuncBindData> copy() const override {
        auto bindData = std::make_unique<ExportParquetBindData>(columnNames, fileName);
        bindData->types = LogicalType::copy(types);
        return bindData;
    }
};

struct ExportParquetLocalState final : public ExportFuncLocalState {
    std::unique_ptr<FactorizedTable> ft;
    uint64_t numTuplesInFT;
    storage::MemoryManager* mm;

    ExportParquetLocalState(const ExportFuncBindData& bindData, main::ClientContext& context,
        std::vector<bool> isFlatVec)
        : mm{context.getMemoryManager()} {
        auto tableSchema = FactorizedTableSchema();
        for (auto i = 0u; i < isFlatVec.size(); i++) {
            auto columnSchema =
                isFlatVec[i] ?
                    ColumnSchema(false, 0 /* dummyGroupPos */,
                        LogicalTypeUtils::getRowLayoutSize(bindData.types[i])) :
                    ColumnSchema(true, 1 /* dummyGroupPos */, (uint32_t)sizeof(overflow_value_t));
            tableSchema.appendColumn(std::move(columnSchema));
        }
        ft = std::make_unique<FactorizedTable>(mm, tableSchema.copy());
        numTuplesInFT = 0;
    }
};

struct ExportParquetSharedState : public ExportFuncSharedState {
    std::unique_ptr<ParquetWriter> writer;

    ExportParquetSharedState(const ExportFuncBindData& bindData, main::ClientContext& context) {
        auto& exportParquetBindData = bindData.constCast<ExportParquetBindData>();
        writer = std::make_unique<ParquetWriter>(exportParquetBindData.fileName,
            common::LogicalType::copy(exportParquetBindData.types),
            exportParquetBindData.columnNames, exportParquetBindData.codec, &context);
    }
};

static std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput) {
    return std::make_unique<ExportParquetBindData>(bindInput.columnNames, bindInput.filePath);
}

static std::unique_ptr<ExportFuncLocalState> initLocalState(main::ClientContext& context,
    const ExportFuncBindData& bindData, std::vector<bool> isFlatVec) {
    return std::make_unique<ExportParquetLocalState>(bindData, context, isFlatVec);
}

static std::shared_ptr<ExportFuncSharedState> initSharedState(main::ClientContext& context,
    const ExportFuncBindData& bindData) {
    return std::make_shared<ExportParquetSharedState>(bindData, context);
}

static std::vector<ValueVector*> extractSharedPtr(
    std::vector<std::shared_ptr<ValueVector>> inputVectors, uint64_t& numTuplesToAppend) {
    std::vector<ValueVector*> vecs;
    numTuplesToAppend =
        inputVectors.size() > 0 ? inputVectors[0]->state->getSelVector().getSelSize() : 0;
    for (auto& inputVector : inputVectors) {
        if (!inputVector->state->isFlat()) {
            numTuplesToAppend = inputVector->state->getSelVector().getSelSize();
        }
        vecs.push_back(inputVector.get());
    }
    return vecs;
}

static void sinkFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState,
    const ExportFuncBindData& /*bindData*/,
    std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& exportParquetLocalState = localState.cast<ExportParquetLocalState>();
    uint64_t numTuplesToAppend = 0;
    // TODO(Ziyi): We should let factorizedTable::append return the numTuples appended.
    exportParquetLocalState.ft->append(extractSharedPtr(inputVectors, numTuplesToAppend));
    exportParquetLocalState.numTuplesInFT += numTuplesToAppend;
    if (exportParquetLocalState.numTuplesInFT > StorageConstants::NODE_GROUP_SIZE) {
        auto& exportParquetSharedState = sharedState.cast<ExportParquetSharedState>();
        exportParquetSharedState.writer->flush(*exportParquetLocalState.ft);
        exportParquetLocalState.numTuplesInFT = 0;
    }
}

static void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState) {
    auto& exportParquetSharedState = sharedState.cast<ExportParquetSharedState>();
    auto& exportParquetLocalState = localState.cast<ExportParquetLocalState>();
    exportParquetSharedState.writer->flush(*exportParquetLocalState.ft);
}

static void finalizeFunc(ExportFuncSharedState& sharedState) {
    auto& exportParquetSharedState = sharedState.cast<ExportParquetSharedState>();
    exportParquetSharedState.writer->finalize();
}

function_set ExportParquetFunction::getFunctionSet() {
    function_set functionSet;
    auto exportFunc = std::make_unique<ExportFunction>(name);
    exportFunc->initLocal = initLocalState;
    exportFunc->initShared = initSharedState;
    exportFunc->sink = sinkFunc;
    exportFunc->combine = combineFunc;
    exportFunc->finalize = finalizeFunc;
    exportFunc->bind = bindFunc;
    functionSet.push_back(std::move(exportFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
