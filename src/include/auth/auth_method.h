#pragma once

#include <cstdint>

#include "auth_info.h"
#include "auth_token.h"
#include "common/types/uuid.h"

namespace kuzu {
namespace auth {

enum class AuthStatus : uint8_t {
    SUCCESS = 0,
    FAIL = 1,
    NOT_ACCEPTED = 2,
};

struct AuthResult {
    AuthStatus status;
    std::string message;
    AuthToken authToken;
};

class KUZU_API AuthMethod {
public:
    virtual AuthResult authenticate(std::unique_ptr<AuthInfo> authInfo) = 0;

    virtual ~AuthMethod() = default;
};

} // namespace auth
} // namespace kuzu
