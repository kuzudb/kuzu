#pragma once

#include <vector>

#include "common/types/types.h"
#include "main/client_context.h"
#include "processor/operator/persistent/reader/csv/csv_error.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct WarningSchema {
    static constexpr uint64_t getNumColumns() { return 4; }
    static std::vector<std::string> getColumnNames();
    static std::vector<common::LogicalType> getColumnDataTypes();
};

struct WarningContext {
    std::shared_ptr<FactorizedTable> warningTable;
    uint64_t warningLimit;
    main::ClientContext* clientContext;
    std::mutex mtx;

    explicit WarningContext(main::ClientContext* clientContext);

    void appendWarningMessages(const std::vector<PopulatedCSVError>& messages,
        uint64_t messageLimit);
};

} // namespace processor
} // namespace kuzu
