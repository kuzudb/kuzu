#pragma once

#include "common/task_system/task.h"
#include "function/table_functions.h"
// TODO(Semih): Remove
#include <iostream>

namespace kuzu {
namespace function {

class GDSTaskSharedState {
public:
    static constexpr const int64_t MORSEL_SIZE = 1000000;
    GDSTaskSharedState(int64_t count) : count{count} {
        nextMorsel.store(0);
        sum.store(0);
    };

public:
    int64_t count;
    std::atomic<int64_t> nextMorsel;
    std::atomic<int64_t> sum;
};

class GDSTask : public common::Task {

public:
    GDSTask(uint64_t maxNumThreads, std::shared_ptr<GDSTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, sharedState{std::move(sharedState)} {
        std::cout << "GDSTask is constructed. maxNumThreads: " << maxNumThreads << std::endl;
    }

    void run() override;
    // TODO(Semih): Remove
    void finalizeIfNecessary() override;

private:
    std::shared_ptr<GDSTaskSharedState> sharedState;
};

} // namespace function
} // namespace kuzu
