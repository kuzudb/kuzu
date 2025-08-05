#pragma once

#include <string>

#include "common/types/uuid.h"

namespace kuzu {
namespace auth {

struct AuthToken {
    std::string token;
};

} // namespace auth
} // namespace kuzu
