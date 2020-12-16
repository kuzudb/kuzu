#pragma once

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace common {

//! The capacity of vector values is dependent on how the vector is produced.
//!  Scans produce vectors in chunks of 1024 nodes while extends leads to the
//!  the max size of an adjacency list which is 2048 nodes.
#define NODE_SEQUENCE_VECTOR_SIZE 1024
#define VECTOR_CAPACITY 2048

//! A Vector represents values of the same data type.
class ValueVector {};

} // namespace common
} // namespace graphflow
