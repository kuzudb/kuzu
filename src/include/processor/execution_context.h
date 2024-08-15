#pragma once

#include "common/profiler.h"
#include "main/client_context.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct PopulatedCSVError;

struct ExecutionContext {
    uint64_t queryID;
    common::Profiler* profiler;
    main::ClientContext* clientContext;
    std::shared_ptr<FactorizedTable> warningTable;
    uint64_t warningLimit;
    std::mutex mtx;

    ExecutionContext(common::Profiler* profiler, main::ClientContext* clientContext,
        uint64_t queryID);

    void setWarningMessages(const std::vector<PopulatedCSVError>& messages, uint64_t messageLimit);
    std::shared_ptr<FactorizedTable>& getWarningTable();
    uint64_t getWarningLimit() const;
};

} // namespace processor
} // namespace kuzu
