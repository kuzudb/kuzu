#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct CastFunctionBindData : public FunctionBindData {
    // We don't allow configuring delimiters, ... in CAST function.
    // For performance purpose, we generate a default option object during binding time.
    common::CSVOption option;
    // TODO(Mahn): the following field should be removed once we refactor fixed list.
    uint64_t numOfEntries;

    explicit CastFunctionBindData(std::unique_ptr<common::LogicalType> dataType)
        : FunctionBindData{std::move(dataType)} {}
};

} // namespace function
} // namespace kuzu
