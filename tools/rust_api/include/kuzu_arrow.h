#pragma once

#include "rust/cxx.h"
#ifdef KUZU_BUNDLED
#include "main/kuzu.h"
#else
#include <kuzu.hpp>
#endif

namespace kuzu_arrow {

ArrowSchema query_result_get_arrow_schema(const kuzu::main::QueryResult& result);
ArrowArray query_result_get_next_arrow_chunk(kuzu::main::QueryResult& result, uint64_t chunkSize);

} // namespace kuzu_arrow
