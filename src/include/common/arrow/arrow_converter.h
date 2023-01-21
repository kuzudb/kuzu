#pragma once

#include <string>
#include <vector>

#include "common/arrow/arrow.h"
#include "common/types/types.h"
#include "main/query_result.h"

struct ArrowSchema;

namespace kuzu {
namespace common {

struct ArrowConverter {
    static void toArrowSchema(
        ArrowSchema* out_schema, std::vector<DataType>& types, std::vector<std::string>& names);
    static void toArrowArray(
        main::QueryResult& queryResult, ArrowArray* out_array, std::int64_t chunkSize);
};

} // namespace common
} // namespace kuzu
