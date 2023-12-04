#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace processor {

struct ReaderBindUtils {
    static void validateNumColumns(uint32_t expectedNumber, uint32_t detectedNumber);
    static void validateColumnTypes(const std::vector<std::string>& columnNames,
        const std::vector<std::unique_ptr<common::LogicalType>>& expectedColumnTypes,
        const std::vector<std::unique_ptr<common::LogicalType>>& detectedColumnTypes);
    static void resolveColumns(const std::vector<std::string>& expectedColumnNames,
        const std::vector<std::string>& detectedColumnNames,
        std::vector<std::string>& resultColumnNames,
        const std::vector<std::unique_ptr<common::LogicalType>>& expectedColumnTypes,
        const std::vector<std::unique_ptr<common::LogicalType>>& detectedColumnTypes,
        std::vector<std::unique_ptr<common::LogicalType>>& resultColumnTypes);
};

} // namespace processor
} // namespace kuzu
