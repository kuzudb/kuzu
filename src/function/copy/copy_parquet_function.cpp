
#include "function/copy/copy_function.h"
#include "main/client_context.h"
#include "parquet/parquet_types.h"
#include "processor/operator/persistent/writer/parquet/parquet_writer.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace function {

using namespace common;
using namespace processor;

struct CopyToParquetBindData final : public CopyFuncBindData {
    kuzu_parquet::format::CompressionCodec::type codec =
        kuzu_parquet::format::CompressionCodec::SNAPPY;

    CopyToParquetBindData(std::vector<std::string> names, std::string fileName, bool canParallel)
        : CopyFuncBindData{std::move(names), std::move(fileName), canParallel} {}

    std::unique_ptr<CopyFuncBindData> copy() const override {
        auto bindData = std::make_unique<CopyToParquetBindData>(names, fileName, canParallel);
        bindData->types = types;
        return bindData;
    }
};

struct CopyToParquetLocalState final : public CopyFuncLocalState {
    std::unique_ptr<FactorizedTable> ft;
    uint64_t numTuplesInFT;
    storage::MemoryManager* mm;

    CopyToParquetLocalState(const CopyFuncBindData& bindData, main::ClientContext& context,
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

struct CopyToParquetSharedState : public CopyFuncSharedState {
    std::unique_ptr<ParquetWriter> writer;

    CopyToParquetSharedState(const CopyFuncBindData& bindData, main::ClientContext& context) {
        auto& copyToBindData = bindData.constCast<CopyToParquetBindData>();
        writer = std::make_unique<ParquetWriter>(copyToBindData.fileName, copyToBindData.types,
            copyToBindData.names, copyToBindData.codec, &context);
    }
};

static std::unique_ptr<CopyFuncBindData> bindFunc(CopyFuncBindInput& bindInput) {
    return std::make_unique<CopyToParquetBindData>(bindInput.columnNames, bindInput.filePath,
        bindInput.canParallel);
}

static std::unique_ptr<CopyFuncLocalState> initLocalState(main::ClientContext& context,
    const CopyFuncBindData& bindData, std::vector<bool> isFlatVec) {
    return std::make_unique<CopyToParquetLocalState>(bindData, context, isFlatVec);
}

static std::shared_ptr<CopyFuncSharedState> initSharedState(main::ClientContext& context,
    const CopyFuncBindData& bindData) {
    return std::make_shared<CopyToParquetSharedState>(bindData, context);
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

static void sinkFunc(CopyFuncSharedState& sharedState, CopyFuncLocalState& localState,
    const CopyFuncBindData& /*bindData*/, std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& copyToLocalState = localState.cast<CopyToParquetLocalState>();
    uint64_t numTuplesToAppend = 0;
    // TODO(Ziyi): We should let factorizedTable::append return the numTuples appended.
    copyToLocalState.ft->append(extractSharedPtr(inputVectors, numTuplesToAppend));
    copyToLocalState.numTuplesInFT += numTuplesToAppend;
    if (copyToLocalState.numTuplesInFT > StorageConstants::NODE_GROUP_SIZE) {
        auto& copyToSharedState = sharedState.cast<CopyToParquetSharedState>();
        copyToSharedState.writer->flush(*copyToLocalState.ft);
        copyToLocalState.numTuplesInFT = 0;
    }
}

static void combineFunc(CopyFuncSharedState& sharedState, CopyFuncLocalState& localState) {
    auto& copyToSharedState = sharedState.cast<CopyToParquetSharedState>();
    auto& copyToLocalState = localState.cast<CopyToParquetLocalState>();
    copyToSharedState.writer->flush(*copyToLocalState.ft);
}

static void finalizeFunc(CopyFuncSharedState& sharedState) {
    auto& copyToSharedState = sharedState.cast<CopyToParquetSharedState>();
    copyToSharedState.writer->finalize();
}

function_set CopyParquetFunction::getFunctionSet() {
    function_set functionSet;
    auto copyFunc = std::make_unique<CopyFunction>(name);
    copyFunc->copyToInitLocal = initLocalState;
    copyFunc->copyToInitShared = initSharedState;
    copyFunc->copyToSink = sinkFunc;
    copyFunc->copyToCombine = combineFunc;
    copyFunc->copyToFinalize = finalizeFunc;
    copyFunc->copyToBind = bindFunc;
    functionSet.push_back(std::move(copyFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
