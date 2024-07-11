#pragma once

#include "common/assert.h"
#include <string>
#include <cstdint>

namespace kuzu {
namespace storage {

enum class ResidencyState : uint8_t { IN_MEMORY = 0, ON_DISK = 1 };

struct ResidencyStateUtils {
    static std::string toString(ResidencyState residencyState) {
      switch (residencyState) {
      case ResidencyState::IN_MEMORY: {
          return "IN_MEMORY";
      }
      case ResidencyState::ON_DISK: {
          return "ON_DISK";
      }
      default: {
          KU_UNREACHABLE;
      }
      }
  }
};

} // namespace storage
} // namespace kuzu
