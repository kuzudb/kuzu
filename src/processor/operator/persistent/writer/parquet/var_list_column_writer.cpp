#include "processor/operator/persistent/writer/parquet/var_list_column_writer.h"

namespace kuzu {
namespace processor {

using namespace kuzu_parquet::format;

std::unique_ptr<ColumnWriterState> VarListColumnWriter::initializeWriteState(
    kuzu_parquet::format::RowGroup& rowGroup) {
    auto result = std::make_unique<ListColumnWriterState>(rowGroup, rowGroup.columns.size());
    result->childState = childWriter->initializeWriteState(rowGroup);
    return std::move(result);
}

bool VarListColumnWriter::hasAnalyze() {
    return childWriter->hasAnalyze();
}

void VarListColumnWriter::analyze(ColumnWriterState& writerState, ColumnWriterState* /*parent*/,
    common::ValueVector* vector, uint64_t /*count*/) {
    auto& state = reinterpret_cast<ListColumnWriterState&>(writerState);
    childWriter->analyze(*state.childState, &writerState, common::ListVector::getDataVector(vector),
        common::ListVector::getDataVectorSize(vector));
}

void VarListColumnWriter::finalizeAnalyze(ColumnWriterState& writerState) {
    auto& state = reinterpret_cast<ListColumnWriterState&>(writerState);
    childWriter->finalizeAnalyze(*state.childState);
}

void VarListColumnWriter::prepare(ColumnWriterState& writerState, ColumnWriterState* parent,
    common::ValueVector* vector, uint64_t count) {
    auto& state = reinterpret_cast<ListColumnWriterState&>(writerState);

    // Write definition levels and repeats.
    uint64_t start = 0;
    auto vcount = parent ? parent->definitionLevels.size() - state.parentIdx : count;
    uint64_t vectorIdx = 0;
    for (auto i = start; i < vcount; i++) {
        auto parentIdx = state.parentIdx + i;
        if (parent && !parent->isEmpty.empty() && parent->isEmpty[parentIdx]) {
            state.definitionLevels.push_back(parent->definitionLevels[parentIdx]);
            state.repetitionLevels.push_back(parent->repetitionLevels[parentIdx]);
            state.isEmpty.push_back(true);
            continue;
        }
        auto firstRepeatLevel = parent && !parent->repetitionLevels.empty() ?
                                    parent->repetitionLevels[parentIdx] :
                                    maxRepeat;
        auto pos = getVectorPos(vector, vectorIdx);
        if (parent &&
            parent->definitionLevels[parentIdx] != common::ParquetConstants::PARQUET_DEFINE_VALID) {
            state.definitionLevels.push_back(parent->definitionLevels[parentIdx]);
            state.repetitionLevels.push_back(firstRepeatLevel);
            state.isEmpty.push_back(true);
        } else if (!vector->isNull(pos)) {
            auto listEntry = vector->getValue<common::list_entry_t>(pos);
            // push the repetition levels
            if (listEntry.size == 0) {
                state.definitionLevels.push_back(maxDefine);
                state.isEmpty.push_back(true);
            } else {
                state.definitionLevels.push_back(common::ParquetConstants::PARQUET_DEFINE_VALID);
                state.isEmpty.push_back(false);
            }
            state.repetitionLevels.push_back(firstRepeatLevel);
            for (auto k = 1; k < listEntry.size; k++) {
                state.repetitionLevels.push_back(maxRepeat + 1);
                state.definitionLevels.push_back(common::ParquetConstants::PARQUET_DEFINE_VALID);
                state.isEmpty.push_back(false);
            }
        } else {
            if (!canHaveNulls) {
                throw common::RuntimeException(
                    "Parquet writer: map key column is not allowed to contain NULL values");
            }
            state.definitionLevels.push_back(maxDefine - 1);
            state.repetitionLevels.push_back(firstRepeatLevel);
            state.isEmpty.push_back(true);
        }
        vectorIdx++;
    }
    state.parentIdx += vcount;
    childWriter->prepare(*state.childState, &writerState, common::ListVector::getDataVector(vector),
        common::ListVector::getDataVectorSize(vector));
}

void VarListColumnWriter::beginWrite(ColumnWriterState& state_p) {
    auto& state = reinterpret_cast<ListColumnWriterState&>(state_p);
    childWriter->beginWrite(*state.childState);
}

void VarListColumnWriter::write(
    ColumnWriterState& writerState, common::ValueVector* vector, uint64_t /*count*/) {
    auto& state = reinterpret_cast<ListColumnWriterState&>(writerState);
    childWriter->write(*state.childState, common::ListVector::getDataVector(vector),
        common::ListVector::getDataVectorSize(vector));
}

void VarListColumnWriter::finalizeWrite(ColumnWriterState& writerState) {
    auto& state = reinterpret_cast<ListColumnWriterState&>(writerState);
    childWriter->finalizeWrite(*state.childState);
}

} // namespace processor
} // namespace kuzu
