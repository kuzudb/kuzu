#include <mutex>
#include <stack>

#include "common/concurrent_vector.h"
#include "common/mpsc_queue.h"
#include <benchmark/benchmark.h>

static kuzu::common::ConcurrentStack<uint32_t> pageStack;
static std::stack<uint32_t> lockedPageStack;
static std::mutex mtx;
static kuzu::common::ConcurrentVector<uint32_t> pageVectorStack(0, 0);

static void Stack(benchmark::State& state) {
    for (auto _ : state) {
        pageStack.push(0);
    }
}

static void LockedStack(benchmark::State& state) {
    for (auto _ : state) {
        std::unique_lock lck{mtx};
        lockedPageStack.push(0);
    }
}

static void ConcurrentVectorStack(benchmark::State& state) {
    for (auto _ : state) {
        pageVectorStack.push_back(0);
    }
}

BENCHMARK(Stack)->ThreadRange(1, 128);
BENCHMARK(LockedStack)->ThreadRange(1, 128);
BENCHMARK(ConcurrentVectorStack)->ThreadRange(1, 128);

BENCHMARK_MAIN();
