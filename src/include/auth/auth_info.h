#pragma once

#include <string>

#include "common/api.h"
#include "common/cast.h"

namespace kuzu {
namespace auth {

struct KUZU_API AuthInfo {
    std::string userName;
    std::string authType;

    explicit AuthInfo(std::string authType) : authType{std::move(authType)} {}

    virtual ~AuthInfo() = default;

    template<class TARGET>
    KUZU_API const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
};

} // namespace auth
} // namespace kuzu
