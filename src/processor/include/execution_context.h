#pragma once

#include "src/common/include/profiler.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::transaction;

namespace graphflow {
namespace processor {

struct ExecutionContext {

public:
    ExecutionContext(Profiler& profiler, Transaction* transaction)
        : profiler{profiler}, transaction{transaction} {}

public:
    Profiler& profiler;
    Transaction* transaction;
};

} // namespace processor
} // namespace graphflow
