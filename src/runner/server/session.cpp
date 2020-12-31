#include "src/runner/include/server/session.h"

namespace graphflow {
namespace runner {

void Session::loadGraph(const string& path) {
    if (nullptr == graph || 0 != path.compare(graph->getPath())) {
        graph.reset();
        graph = make_unique<Graph>(path, bufferPoolSize);
        return;
    }
    throw invalid_argument("Graph is already initialized.");
}

void Session::setBufferPoolSize(uint64_t size) {
    if (size < (1 << 20) || size > (1ul << 40u)) {
        throw invalid_argument(
            "Buffer pool size argument should be graeter than 1MB or less than 1TB.");
    }
    if (0 != size % PAGE_SIZE) {
        throw invalid_argument(
            "Buffer pool size should be aligned to the system PAGE SIZE (=4096B)");
    }
    if (bufferPoolSize == size) {
        throw invalid_argument("no Change in size.");
    }
    bufferPoolSize = size;
    graph.reset();
}

} // namespace runner
} // namespace graphflow