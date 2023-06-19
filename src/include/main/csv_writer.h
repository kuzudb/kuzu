#pragma once

#include "common/api.h"
#include "common/types/types.h"
#include "main/query_result.h"

namespace kuzu {
namespace main {

class CSVWriter {
public:
    static void writeToCSV(main::QueryResult& queryResult, const std::string& fileName,
        char delimiter, char escapeCharacter, char newline);
};

} // namespace main
} // namespace kuzu
