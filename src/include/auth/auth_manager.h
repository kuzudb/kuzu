#pragma once

#include <memory>
#include <vector>

#include "auth_method.h"
#include "common/enums/statement_type.h"

namespace kuzu {
namespace auth {

class AuthManager {

public:
    virtual ~AuthManager() = default;

    void registerAuthMethod(std::unique_ptr<AuthMethod>);

    virtual AuthResult authenticate(std::unique_ptr<auth::AuthInfo> authInfo);

    virtual bool hasAccess(AuthToken authToken, common::StatementType statementType) const;

    virtual void createUser();

protected:
    std::vector<std::unique_ptr<AuthMethod>> authMethods;
};

} // namespace auth
} // namespace kuzu
