#pragma once

#include "function/function.h"

namespace kuzu {
namespace fts_extension {

std::vector<std::string> getTerms(std::string& query, const std::string& stemmer);

} // namespace fts_extension
} // namespace kuzu
