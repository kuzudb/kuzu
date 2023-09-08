#pragma once

#include "processor/data_pos.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct OrderByDataInfo {
    std::vector<DataPos> keysPos;
    std::vector<DataPos> payloadsPos;
    std::vector<std::unique_ptr<common::LogicalType>> keyTypes;
    std::vector<std::unique_ptr<common::LogicalType>> payloadTypes;
    std::vector<bool> isAscOrder;
    std::unique_ptr<FactorizedTableSchema> payloadTableSchema;
    std::vector<uint32_t> keyInPayloadPos;

    OrderByDataInfo(std::vector<DataPos> keysPos, std::vector<DataPos> payloadsPos,
        std::vector<std::unique_ptr<common::LogicalType>> keyTypes,
        std::vector<std::unique_ptr<common::LogicalType>> payloadTypes,
        std::vector<bool> isAscOrder, std::unique_ptr<FactorizedTableSchema> payloadTableSchema,
        std::vector<uint32_t> keyInPayloadPos)
        : keysPos{std::move(keysPos)}, payloadsPos{std::move(payloadsPos)}, keyTypes{std::move(
                                                                                keyTypes)},
          payloadTypes{std::move(payloadTypes)}, isAscOrder{std::move(isAscOrder)},
          payloadTableSchema{std::move(payloadTableSchema)}, keyInPayloadPos{
                                                                 std::move(keyInPayloadPos)} {}
    OrderByDataInfo(const OrderByDataInfo& other)
        : keysPos{other.keysPos},
          payloadsPos{other.payloadsPos}, keyTypes{common::LogicalType::copy(other.keyTypes)},
          payloadTypes{common::LogicalType::copy(other.payloadTypes)}, isAscOrder{other.isAscOrder},
          payloadTableSchema{other.payloadTableSchema->copy()}, keyInPayloadPos{
                                                                    other.keyInPayloadPos} {}

    std::unique_ptr<OrderByDataInfo> copy() const {
        return std::make_unique<OrderByDataInfo>(*this);
    }
};

} // namespace processor
} // namespace kuzu
