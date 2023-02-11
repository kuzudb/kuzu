#include "main/client_context.h"

#include <thread>

namespace kuzu {
namespace main {

ClientContext::ClientContext() : numThreadsForExecution{std::thread::hardware_concurrency()} {}

} // namespace main
} // namespace kuzu
