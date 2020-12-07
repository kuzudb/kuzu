#pragma once

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace common {

//! Vector size should be a power of two. For now fixed to the value below.
#define VECTOR_SIZE 1024

//! A Vector represents values of the same data type.
class ValueVector {};

} // namespace common
} // namespace graphflow
