#include "include/types.h"

namespace graphflow {
namespace common {

Direction operator!(Direction& direction) {
    return (FWD == direction) ? BWD : FWD;
}

} // namespace common
} // namespace graphflow
