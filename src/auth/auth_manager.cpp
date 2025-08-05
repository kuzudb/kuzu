#include "auth/auth_manager.h"

namespace kuzu {
namespace auth {

void AuthManager::registerAuthMethod(std::unique_ptr<AuthMethod> authMethod) {
    authMethods.push_back(std::move(authMethod));
}

AuthResult AuthManager::authenticate(std::unique_ptr<auth::AuthInfo> authInfo) {
    for (auto& authMethod : authMethods) {
        auto authResult = authMethod->authenticate(std::move(authInfo));
        switch (authResult.status) {
        case AuthStatus::SUCCESS:
        case AuthStatus::FAIL:
            return authResult;
        case AuthStatus::NOT_ACCEPTED:
            continue;
        }
    }
    return AuthResult{AuthStatus::NOT_ACCEPTED,
        "No authentication method registered that can handle the provided authentication "
        "information. "
        "Please double check whether the authInfo is constructed properly.",
        {""}};
}

bool AuthManager::hasAccess(AuthToken authToken, common::StatementType statementType) const {
    return true;
}

void AuthManager::createUser() {
    KU_UNREACHABLE;
}

} // namespace auth
} // namespace kuzu
