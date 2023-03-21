#include "main/client_context.h"

#include <thread>

namespace kuzu {
namespace main {

ClientContext::ClientContext()
    : numThreadsForExecution{std::thread::hardware_concurrency()}, interrupted{false} {}

} // namespace main
} // namespace kuzu
