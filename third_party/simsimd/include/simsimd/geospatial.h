/**
 *  @file       geospatial.h
 *  @brief      SIMD-accelerated Geo-Spatial distance functions.
 *  @author     Ash Vardanian
 *  @date       July 1, 2023
 *
 *  Contains:
 *  - Haversine (Great Circle) distance
 *  - TODO: Vincenty's distance function for Oblate Spheroid Geodesics
 *
 *  For datatypes:
 *  - 32-bit IEEE-754 floating point
 *  - 64-bit IEEE-754 floating point
 *
 *  For hardware architectures:
 *  - Arm (NEON, SVE)
 *  - x86 (AVX512)
 *
 *  x86 intrinsics: https://www.intel.com/content/www/us/en/docs/intrinsics-guide/
 *  Arm intrinsics: https://developer.arm.com/architectures/instruction-sets/intrinsics/
 *  Oblate Spheroid Geodesic: https://mathworld.wolfram.com/OblateSpheroidGeodesic.html
 *  Staging experiments: https://github.com/ashvardanian/HaversineSimSIMD
 */
#ifndef SIMSIMD_GEOSPATIAL_H
#define SIMSIMD_GEOSPATIAL_H

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif
