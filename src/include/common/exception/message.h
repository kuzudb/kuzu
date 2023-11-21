#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

struct ExceptionMessage {
    static std::string existedPKException(const std::string& pkString);
    static std::string nonExistPKException(const std::string& pkString);
    static std::string invalidPKType(const std::string& type);
    static inline std::string nullPKException() {
        return "Found NULL, which violates the non-null constraint of the primary key column.";
    }
    static inline std::string notAllowCopyOnNonEmptyTableException() {
        return "COPY commands can only be executed once on a table.";
    }
    static std::string overLargeStringPKValueException(uint64_t length);
    static std::string overLargeStringValueException(uint64_t length);
    static std::string violateUniquenessOfRelAdjColumn(const std::string& tableName,
        const std::string& offset, const std::string& multiplicity, const std::string& direction);

    static inline std::string validateCopyNPYByColumnException() {
        return "Please use COPY FROM BY COLUMN statement for copying npy files.";
    }
    static inline std::string validateCopyCSVParquetByColumnException() {
        return "Please use COPY FROM statement for copying csv and parquet files.";
    }
    static inline std::string validateCopyToCSVParquetExtensionsException() {
        return "COPY TO currently only supports csv and parquet files.";
    }
    static std::string validateCopyNpyNotForRelTablesException(const std::string& tableName);
};

} // namespace common
} // namespace kuzu
