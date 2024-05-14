#include "processor/result/factorized_table_util.h"

using namespace kuzu::storage;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

FactorizedTableSchema FactorizedTableUtils::createFTableSchema(const expression_vector& exprs,
    const Schema& schema) {
    auto tableSchema = FactorizedTableSchema();
    std::unordered_set<common::idx_t> groupIDSet;
    for (auto& e : exprs) {
        auto groupPos = schema.getExpressionPos(*e).first;
        auto group = schema.getGroup(groupPos);
        if (group->isFlat()) {
            auto column =
                ColumnSchema(false, groupPos, LogicalTypeUtils::getRowLayoutSize(e->getDataType()));
            tableSchema.appendColumn(std::move(column));
        } else {
            auto column = ColumnSchema(true, groupPos, (uint32_t)sizeof(overflow_value_t));
            tableSchema.appendColumn(std::move(column));
        }
    }
    return tableSchema;
}

FactorizedTableSchema FactorizedTableUtils::createFlatTableSchema(
    std::vector<LogicalType> columnTypes) {
    auto tableSchema = FactorizedTableSchema();
    for (auto& type : columnTypes) {
        auto column = ColumnSchema(false /* isUnFlat */, 0 /* groupID */,
            LogicalTypeUtils::getRowLayoutSize(type));
        tableSchema.appendColumn(std::move(column));
    }
    return tableSchema;
}

void FactorizedTableUtils::appendStringToTable(FactorizedTable* factorizedTable,
    std::string& outputMsg, MemoryManager* memoryManager) {
    auto outputMsgVector = std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager);
    outputMsgVector->state = DataChunkState::getSingleValueDataChunkState();
    auto outputKUStr = ku_string_t();
    outputKUStr.overflowPtr =
        reinterpret_cast<uint64_t>(StringVector::getInMemOverflowBuffer(outputMsgVector.get())
                                       ->allocateSpace(outputMsg.length()));
    outputKUStr.set(outputMsg);
    outputMsgVector->setValue(0, outputKUStr);
    factorizedTable->append(std::vector<ValueVector*>{outputMsgVector.get()});
}

std::shared_ptr<FactorizedTable> FactorizedTableUtils::getFactorizedTableForOutputMsg(
    std::string& outputMsg, MemoryManager* memoryManager) {
    auto table = getSingleStringColumnFTable(memoryManager);
    appendStringToTable(table.get(), outputMsg, memoryManager);
    return table;
}

std::shared_ptr<FactorizedTable> FactorizedTableUtils::getSingleStringColumnFTable(
    MemoryManager* mm) {
    auto fTableSchema = createFlatTableSchema(std::vector<LogicalType>{*LogicalType::STRING()});
    return std::make_shared<FactorizedTable>(mm, std::move(fTableSchema));
}

} // namespace processor
} // namespace kuzu
