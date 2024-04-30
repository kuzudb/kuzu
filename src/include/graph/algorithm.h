#include <atomic>
#include <deque>
#include <condition_variable>

#include "algorithm_tasks.h"
#include "graph.h"

namespace kuzu {
namespace graph {

enum AlgorithmResultType {
    PATH_LENGTH,
    PATH
};

struct AlgorithmConfig {
    uint64_t maxThreadsForExecution;
    uint64_t maxActiveTasks;
    // add other configuration
};

struct AlgorithmResult {
    // Only path length
    // Or full path
};

class Algorithm {
public:
    virtual ~Algorithm() = default;

    // main function where all threads start from
    virtual void startExecute() final;

    // all worker threads will keep spinning here and pulling work from queue
    virtual void workerThreadFunction();

    // determine how to pull work from the pending tasks queue, policy dependent on algorithm
    virtual AlgorithmTask* getJobFromQueue();

    // called by the main thread to begin the computation
    virtual void compute();

    // called from compute to add tasks
    virtual void addTasksToQueue();

private:
    std::mutex mtx;
    std::atomic_bool isActive;
    std::condition_variable cv;
    std::deque<std::unique_ptr<AlgorithmTask>> pendingAlgorithmTasks;

    std::shared_ptr<Graph> graph;
    uint64_t totalThreadsRegistered;
    bool masterIsSelected;
    AlgorithmConfig config;
    AlgorithmResult finalResult;
};


}
}