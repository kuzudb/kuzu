#pragma once

#include <string>

namespace kuzu {
namespace parser {

struct AttachInfo {
    std::string dbPath, dbAlias, dbType;
};

} // namespace parser
} // namespace kuzu
