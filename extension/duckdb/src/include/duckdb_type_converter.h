#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBTypeConverter {
public:
    static common::LogicalType convertDuckDBType(std::string typeStr);

private:
    static std::vector<std::string> parseStructFields(const std::string& structTypeStr);
    static std::vector<common::StructField> parseStructTypeInfo(const std::string& structTypeStr);
};

} // namespace duckdb_extension
} // namespace kuzu
