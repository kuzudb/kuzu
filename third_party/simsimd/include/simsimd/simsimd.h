/**
 *  @file       simsimd.h
 *  @brief      SIMD-accelerated Similarity Measures and Distance Functions.
 *  @author     Ash Vardanian
 *  @date       March 14, 2023
 *  @copyright  Copyright (c) 2023
 *
 *  References:
 *  x86 intrinsics: https://www.intel.com/content/www/us/en/docs/intrinsics-guide/
 *  Arm intrinsics: https://developer.arm.com/architectures/instruction-sets/intrinsics/
 *  Detecting target CPU features at compile time: https://stackoverflow.com/a/28939692/2766161
 */

#ifndef SIMSIMD_H
#define SIMSIMD_H

#define SIMSIMD_VERSION_MAJOR 4
#define SIMSIMD_VERSION_MINOR 3
#define SIMSIMD_VERSION_PATCH 1

/**
 *  @brief  Removes compile-time dispatching, and replaces it with runtime dispatching.
 *          So the `simsimd_dot_f32` function will invoke the most advanced backend supported by the CPU,
 *          that runs the program, rather than the most advanced backend supported by the CPU
 *          used to compile the library or the downstream application.
 */
#ifndef SIMSIMD_DYNAMIC_DISPATCH
#define SIMSIMD_DYNAMIC_DISPATCH (0) // true or false
#endif

#include "binary.h"      // Hamming, Jaccard
#include "dot.h"         // Inner (dot) product, and its conjugate
#include "geospatial.h"  // Haversine and Vincenty
#include "probability.h" // Kullback-Leibler, Jensen–Shannon
#include "spatial.h"     // L2, Cosine

#if SIMSIMD_TARGET_ARM
#ifdef __linux__
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

/**
 *  @brief  Enumeration of supported metric kinds.
 *          Some have aliases for convenience.
 */
typedef enum {
    simsimd_metric_unknown_k = 0, ///< Unknown metric kind

    // Classics:
    simsimd_metric_dot_k = 'i',   ///< Inner product
    simsimd_metric_inner_k = 'i', ///< Inner product alias

    simsimd_metric_vdot_k = 'v', ///< Complex inner product

    simsimd_metric_cos_k = 'c',     ///< Cosine similarity
    simsimd_metric_cosine_k = 'c',  ///< Cosine similarity alias
    simsimd_metric_angular_k = 'c', ///< Cosine similarity alias

    simsimd_metric_l2sq_k = 'e',        ///< Squared Euclidean distance
    simsimd_metric_sqeuclidean_k = 'e', ///< Squared Euclidean distance alias

    // Binary:
    simsimd_metric_hamming_k = 'h',   ///< Hamming distance
    simsimd_metric_manhattan_k = 'h', ///< Manhattan distance is same as Hamming

    simsimd_metric_jaccard_k = 'j',  ///< Jaccard coefficient
    simsimd_metric_tanimoto_k = 'j', ///< Tanimoto coefficient is same as Jaccard

    // Probability:
    simsimd_metric_kl_k = 'k',               ///< Kullback-Leibler divergence
    simsimd_metric_kullback_leibler_k = 'k', ///< Kullback-Leibler divergence alias

    simsimd_metric_js_k = 's',             ///< Jensen-Shannon divergence
    simsimd_metric_jensen_shannon_k = 's', ///< Jensen-Shannon divergence alias

} simsimd_metric_kind_t;

/**
 *  @brief  Enumeration of SIMD capabilities of the target architecture.
 */
typedef enum {
    simsimd_cap_serial_k = 1,       ///< Serial (non-SIMD) capability
    simsimd_cap_any_k = 0x7FFFFFFF, ///< Mask representing any capability with `INT_MAX`

    simsimd_cap_neon_k = 1 << 10, ///< ARM NEON capability
    simsimd_cap_sve_k = 1 << 11,  ///< ARM SVE capability
    simsimd_cap_sve2_k = 1 << 12, ///< ARM SVE2 capability

    simsimd_cap_haswell_k = 1 << 20,  ///< x86 AVX2 capability with FMA and F16C extensions
    simsimd_cap_skylake_k = 1 << 21,  ///< x86 AVX512 baseline capability
    simsimd_cap_ice_k = 1 << 22,      ///< x86 AVX512 capability with advanced integer algos
    simsimd_cap_sapphire_k = 1 << 23, ///< x86 AVX512 capability with `f16` support

} simsimd_capability_t;

/**
 *  @brief  Enumeration of supported data types.
 *
 *  Includes complex type descriptors which in C code would use the real counterparts,
 *  but the independent flags contain metadata to be passed between programming language
 *  interfaces.
 */
typedef enum {
    simsimd_datatype_unknown_k, ///< Unknown data type
    simsimd_datatype_f64_k,     ///< Double precision floating point
    simsimd_datatype_f32_k,     ///< Single precision floating point
    simsimd_datatype_f16_k,     ///< Half precision floating point
    simsimd_datatype_i8_k,      ///< 8-bit integer
    simsimd_datatype_b8_k,      ///< Single-bit values packed into 8-bit words

    simsimd_datatype_f64c_k, ///< Complex double precision floating point
    simsimd_datatype_f32c_k, ///< Complex single precision floating point
    simsimd_datatype_f16c_k, ///< Complex half precision floating point
} simsimd_datatype_t;

/**
 *  @brief  Type-punned function pointer accepting two vectors and outputting their similarity/distance.
 *
 *  @param[in] a    Pointer to the first data array.
 *  @param[in] b    Pointer to the second data array.
 *  @param[in] n    Number of scalar words in the input arrays.
 *  @param[out] d   Output value as a double-precision float.
 *                  In complex dot-products @b two double-precision scalars are exported
 *                  for the real and imaginary parts.
 */
typedef void (*simsimd_metric_punned_t)(void const* a, void const* b, simsimd_size_t n, simsimd_distance_t* d);

#if SIMSIMD_DYNAMIC_DISPATCH
SIMSIMD_DYNAMIC simsimd_capability_t simsimd_capabilities(void);
#else
SIMSIMD_PUBLIC simsimd_capability_t simsimd_capabilities(void);
#endif

/**
 *  @brief  Function to determine the SIMD capabilities of the current machine at @b runtime.
 *  @return A bitmask of the SIMD capabilities represented as a `simsimd_capability_t` enum value.
 */
SIMSIMD_PUBLIC simsimd_capability_t simsimd_capabilities_implementation(void) {

#if SIMSIMD_TARGET_X86

    /// The states of 4 registers populated for a specific "cpuid" assembly call
    union four_registers_t {
        int array[4];
        struct separate_t {
            unsigned eax, ebx, ecx, edx;
        } named;
    } info1, info7;

#ifdef _MSC_VER
    __cpuidex(info1.array, 1, 0);
    __cpuidex(info7.array, 7, 0);
#else
    __asm__ __volatile__("cpuid"
                         : "=a"(info1.named.eax), "=b"(info1.named.ebx), "=c"(info1.named.ecx), "=d"(info1.named.edx)
                         : "a"(1), "c"(0));
    __asm__ __volatile__("cpuid"
                         : "=a"(info7.named.eax), "=b"(info7.named.ebx), "=c"(info7.named.ecx), "=d"(info7.named.edx)
                         : "a"(7), "c"(0));
#endif

    // Check for AVX2 (Function ID 7, EBX register)
    // https://github.com/llvm/llvm-project/blob/50598f0ff44f3a4e75706f8c53f3380fe7faa896/clang/lib/Headers/cpuid.h#L148
    unsigned supports_avx2 = (info7.named.ebx & 0x00000020) != 0;
    // Check for F16C (Function ID 1, ECX register)
    // https://github.com/llvm/llvm-project/blob/50598f0ff44f3a4e75706f8c53f3380fe7faa896/clang/lib/Headers/cpuid.h#L107
    unsigned supports_f16c = (info1.named.ecx & 0x20000000) != 0;
    unsigned supports_fma = (info1.named.ecx & 0x00001000) != 0;
    // Check for AVX512F (Function ID 7, EBX register)
    // https://github.com/llvm/llvm-project/blob/50598f0ff44f3a4e75706f8c53f3380fe7faa896/clang/lib/Headers/cpuid.h#L155
    unsigned supports_avx512f = (info7.named.ebx & 0x00010000) != 0;
    // Check for AVX512FP16 (Function ID 7, EDX register)
    // https://github.com/llvm/llvm-project/blob/50598f0ff44f3a4e75706f8c53f3380fe7faa896/clang/lib/Headers/cpuid.h#L198C9-L198C23
    unsigned supports_avx512fp16 = (info7.named.edx & 0x00800000) != 0;
    // Check for AVX512VNNI (Function ID 7, ECX register)
    unsigned supports_avx512vnni = (info7.named.ecx & 0x00000800) != 0;
    // Check for AVX512IFMA (Function ID 7, EBX register)
    unsigned supports_avx512ifma = (info7.named.ebx & 0x00200000) != 0;
    // Check for AVX512BITALG (Function ID 7, ECX register)
    unsigned supports_avx512bitalg = (info7.named.ecx & 0x00001000) != 0;
    // Check for AVX512VBMI2 (Function ID 7, ECX register)
    unsigned supports_avx512vbmi2 = (info7.named.ecx & 0x00000040) != 0;
    // Check for AVX512VPOPCNTDQ (Function ID 7, ECX register)
    unsigned supports_avx512vpopcntdq = (info7.named.ecx & 0x00004000) != 0;

    // Convert specific features into CPU generations
    unsigned supports_haswell = supports_avx2 && supports_f16c && supports_fma;
    unsigned supports_skylake = supports_avx512f;
    unsigned supports_ice = supports_avx512vnni && supports_avx512ifma && supports_avx512bitalg &&
                            supports_avx512vbmi2 && supports_avx512vpopcntdq;
    unsigned supports_sapphire = supports_avx512fp16;

    return (simsimd_capability_t)(                     //
        (simsimd_cap_haswell_k * supports_haswell) |   //
        (simsimd_cap_skylake_k * supports_skylake) |   //
        (simsimd_cap_ice_k * supports_ice) |           //
        (simsimd_cap_sapphire_k * supports_sapphire) | //
        (simsimd_cap_serial_k));

#endif // SIMSIMD_TARGET_X86

#if SIMSIMD_TARGET_ARM

    // Every 64-bit Arm CPU supports NEON
    unsigned supports_neon = 1;
    unsigned supports_sve = 0;
    unsigned supports_sve2 = 0;

#ifdef __linux__
    unsigned long hwcap = getauxval(AT_HWCAP);
    unsigned long hwcap2 = getauxval(AT_HWCAP2);
    supports_sve = (hwcap & HWCAP_SVE) != 0;
    supports_sve2 = (hwcap2 & HWCAP2_SVE2) != 0;
#endif

    return (simsimd_capability_t)(             //
        (simsimd_cap_neon_k * supports_neon) | //
        (simsimd_cap_sve_k * supports_sve) |   //
        (simsimd_cap_sve2_k * supports_sve2) | //
        (simsimd_cap_serial_k));

#endif // SIMSIMD_TARGET_ARM

    return simsimd_cap_serial_k;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-function-type"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wcast-function-type"

/**
 *  @brief  Determines the best suited metric implementation based on the given datatype,
 *          supported and allowed by hardware capabilities.
 *
 *  @param kind The kind of metric to be evaluated.
 *  @param datatype The data type for which the metric needs to be evaluated.
 *  @param supported The hardware capabilities supported by the CPU.
 *  @param allowed The hardware capabilities allowed for use.
 *  @param metric_output Output variable for the selected similarity function.
 *  @param capability_output Output variable for the utilized hardware capabilities.
 */
SIMSIMD_PUBLIC void simsimd_find_metric_punned( //
    simsimd_metric_kind_t kind,                 //
    simsimd_datatype_t datatype,                //
    simsimd_capability_t supported,             //
    simsimd_capability_t allowed,               //
    simsimd_metric_punned_t* metric_output,     //
    simsimd_capability_t* capability_output) {

    simsimd_metric_punned_t* m = metric_output;
    simsimd_capability_t* c = capability_output;
    simsimd_capability_t viable = (simsimd_capability_t)(supported & allowed);
    *m = (simsimd_metric_punned_t)0;
    *c = (simsimd_capability_t)0;

    typedef simsimd_metric_punned_t m_t;
    switch (datatype) {

    case simsimd_datatype_unknown_k: break;

    // Double-precision floating-point vectors
    case simsimd_datatype_f64_k:

#if SIMSIMD_TARGET_SVE
        if (viable & simsimd_cap_skylake_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f64_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f64_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f64_sve, *c = simsimd_cap_sve_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_SKYLAKE
        if (viable & simsimd_cap_skylake_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f64_skylake, *c = simsimd_cap_skylake_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f64_skylake, *c = simsimd_cap_skylake_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f64_skylake, *c = simsimd_cap_skylake_k; return;
            default: break;
            }
#endif
        if (viable & simsimd_cap_serial_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f64_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f64_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f64_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_js_k: *m = (m_t)&simsimd_js_f64_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_kl_k: *m = (m_t)&simsimd_kl_f64_serial, *c = simsimd_cap_serial_k; return;
            default: break;
            }

        break;

    // Single-precision floating-point vectors
    case simsimd_datatype_f32_k:

#if SIMSIMD_TARGET_SVE
        if (viable & simsimd_cap_sve_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f32_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f32_sve, *c = simsimd_cap_sve_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_NEON
        if (viable & simsimd_cap_neon_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f32_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f32_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_js_k: *m = (m_t)&simsimd_js_f32_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_kl_k: *m = (m_t)&simsimd_kl_f32_neon, *c = simsimd_cap_neon_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_SKYLAKE
        if (viable & simsimd_cap_skylake_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32_skylake, *c = simsimd_cap_skylake_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f32_skylake, *c = simsimd_cap_skylake_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f32_skylake, *c = simsimd_cap_skylake_k; return;
            case simsimd_metric_js_k: *m = (m_t)&simsimd_js_f32_skylake, *c = simsimd_cap_skylake_k; return;
            case simsimd_metric_kl_k: *m = (m_t)&simsimd_kl_f32_skylake, *c = simsimd_cap_skylake_k; return;
            default: break;
            }
#endif
        if (viable & simsimd_cap_serial_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f32_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f32_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_js_k: *m = (m_t)&simsimd_js_f32_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_kl_k: *m = (m_t)&simsimd_kl_f32_serial, *c = simsimd_cap_serial_k; return;
            default: break;
            }

        break;

    // Half-precision floating-point vectors
    case simsimd_datatype_f16_k:

#if SIMSIMD_TARGET_SVE
        if (viable & simsimd_cap_sve_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f16_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f16_sve, *c = simsimd_cap_sve_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_NEON
        if (viable & simsimd_cap_neon_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f16_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f16_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_js_k: *m = (m_t)&simsimd_js_f16_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_kl_k: *m = (m_t)&simsimd_kl_f16_neon, *c = simsimd_cap_neon_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_SAPPHIRE
        if (viable & simsimd_cap_sapphire_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16_sapphire, *c = simsimd_cap_sapphire_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f16_sapphire, *c = simsimd_cap_sapphire_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f16_sapphire, *c = simsimd_cap_sapphire_k; return;
            case simsimd_metric_js_k: *m = (m_t)&simsimd_js_f16_sapphire, *c = simsimd_cap_sapphire_k; return;
            case simsimd_metric_kl_k: *m = (m_t)&simsimd_kl_f16_sapphire, *c = simsimd_cap_sapphire_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_HASWELL
        if (viable & simsimd_cap_haswell_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f16_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f16_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_js_k: *m = (m_t)&simsimd_js_f16_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_kl_k: *m = (m_t)&simsimd_kl_f16_haswell, *c = simsimd_cap_haswell_k; return;
            default: break;
            }
#endif

        if (viable & simsimd_cap_serial_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_f16_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_f16_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_js_k: *m = (m_t)&simsimd_js_f16_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_kl_k: *m = (m_t)&simsimd_kl_f16_serial, *c = simsimd_cap_serial_k; return;
            default: break;
            }

        break;

    // Single-byte integer vectors
    case simsimd_datatype_i8_k:
#if SIMSIMD_TARGET_NEON
        if (viable & simsimd_cap_neon_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_cos_i8_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_i8_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_i8_neon, *c = simsimd_cap_neon_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_ICE
        if (viable & simsimd_cap_ice_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_cos_i8_ice, *c = simsimd_cap_ice_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_i8_ice, *c = simsimd_cap_ice_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_i8_ice, *c = simsimd_cap_ice_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_HASWELL
        if (viable & simsimd_cap_haswell_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_cos_i8_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_i8_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_i8_haswell, *c = simsimd_cap_haswell_k; return;
            default: break;
            }
#endif

        if (viable & simsimd_cap_serial_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_cos_i8_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_cos_k: *m = (m_t)&simsimd_cos_i8_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_l2sq_k: *m = (m_t)&simsimd_l2sq_i8_serial, *c = simsimd_cap_serial_k; return;
            default: break;
            }

        break;

    // Binary vectors
    case simsimd_datatype_b8_k:

#if SIMSIMD_TARGET_SVE
        if (viable & simsimd_cap_sve_k)
            switch (kind) {
            case simsimd_metric_hamming_k: *m = (m_t)&simsimd_hamming_b8_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_jaccard_k: *m = (m_t)&simsimd_jaccard_b8_sve, *c = simsimd_cap_sve_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_NEON
        if (viable & simsimd_cap_neon_k)
            switch (kind) {
            case simsimd_metric_hamming_k: *m = (m_t)&simsimd_hamming_b8_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_jaccard_k: *m = (m_t)&simsimd_jaccard_b8_neon, *c = simsimd_cap_neon_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_ICE
        if (viable & simsimd_cap_ice_k)
            switch (kind) {
            case simsimd_metric_hamming_k: *m = (m_t)&simsimd_hamming_b8_ice, *c = simsimd_cap_ice_k; return;
            case simsimd_metric_jaccard_k: *m = (m_t)&simsimd_jaccard_b8_ice, *c = simsimd_cap_ice_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_HASWELL
        if (viable & simsimd_cap_haswell_k)
            switch (kind) {
            case simsimd_metric_hamming_k: *m = (m_t)&simsimd_hamming_b8_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_jaccard_k: *m = (m_t)&simsimd_jaccard_b8_haswell, *c = simsimd_cap_haswell_k; return;
            default: break;
            }
#endif

        if (viable & simsimd_cap_serial_k)
            switch (kind) {
            case simsimd_metric_hamming_k: *m = (m_t)&simsimd_hamming_b8_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_jaccard_k: *m = (m_t)&simsimd_jaccard_b8_serial, *c = simsimd_cap_serial_k; return;
            default: break;
            }

        break;

    case simsimd_datatype_f32c_k:

#if SIMSIMD_TARGET_SVE
        if (viable & simsimd_cap_sve_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32c_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f32c_sve, *c = simsimd_cap_sve_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_NEON
        if (viable & simsimd_cap_neon_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32c_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f32c_neon, *c = simsimd_cap_neon_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_HASWELL
        if (viable & simsimd_cap_haswell_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32c_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f32c_haswell, *c = simsimd_cap_haswell_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_SKYLAKE
        if (viable & simsimd_cap_skylake_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32c_skylake, *c = simsimd_cap_skylake_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f32c_skylake, *c = simsimd_cap_skylake_k; return;
            default: break;
            }
#endif

        if (viable & simsimd_cap_serial_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f32c_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f32c_serial, *c = simsimd_cap_serial_k; return;
            default: break;
            }

        break;

    case simsimd_datatype_f64c_k:

#if SIMSIMD_TARGET_SVE
        if (viable & simsimd_cap_sve_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f64c_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f64c_sve, *c = simsimd_cap_sve_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_SKYLAKE
        if (viable & simsimd_cap_skylake_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f64c_skylake, *c = simsimd_cap_skylake_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f64c_skylake, *c = simsimd_cap_skylake_k; return;
            default: break;
            }
#endif

        if (viable & simsimd_cap_serial_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f64c_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f64c_serial, *c = simsimd_cap_serial_k; return;
            default: break;
            }

        break;

    case simsimd_datatype_f16c_k:

#if SIMSIMD_TARGET_SVE
        if (viable & simsimd_cap_sve_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16c_sve, *c = simsimd_cap_sve_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f16c_sve, *c = simsimd_cap_sve_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_NEON
        if (viable & simsimd_cap_neon_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16c_neon, *c = simsimd_cap_neon_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f16c_neon, *c = simsimd_cap_neon_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_HASWELL
        if (viable & simsimd_cap_haswell_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16c_haswell, *c = simsimd_cap_haswell_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f16c_haswell, *c = simsimd_cap_haswell_k; return;
            default: break;
            }
#endif
#if SIMSIMD_TARGET_SAPPHIRE
        if (viable & simsimd_cap_sapphire_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16c_sapphire, *c = simsimd_cap_sapphire_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f16c_sapphire, *c = simsimd_cap_sapphire_k; return;
            default: break;
            }
#endif

        if (viable & simsimd_cap_serial_k)
            switch (kind) {
            case simsimd_metric_dot_k: *m = (m_t)&simsimd_dot_f16c_serial, *c = simsimd_cap_serial_k; return;
            case simsimd_metric_vdot_k: *m = (m_t)&simsimd_vdot_f16c_serial, *c = simsimd_cap_serial_k; return;
            default: break;
            }

        break;
    }
}

#pragma clang diagnostic pop
#pragma GCC diagnostic pop

/**
 *  @brief  Selects the most suitable metric implementation based on the given metric kind, datatype,
 *          and allowed capabilities. @b Don't call too often and prefer caching the `simsimd_capabilities()`.
 *
 *  @param kind The kind of metric to be evaluated.
 *  @param datatype The data type for which the metric needs to be evaluated.
 *  @param allowed The hardware capabilities allowed for use.
 *  @return A function pointer to the selected metric implementation.
 */
SIMSIMD_PUBLIC simsimd_metric_punned_t simsimd_metric_punned( //
    simsimd_metric_kind_t kind,                               //
    simsimd_datatype_t datatype,                              //
    simsimd_capability_t allowed) {

    simsimd_metric_punned_t result = 0;
    simsimd_capability_t c = simsimd_cap_serial_k;
    simsimd_capability_t supported = simsimd_capabilities();
    simsimd_find_metric_punned(kind, datatype, supported, allowed, &result, &c);
    return result;
}

#if SIMSIMD_DYNAMIC_DISPATCH

/*  Run-time feature-testing functions
 *  - Check if the CPU supports NEON or SVE extensions on Arm
 *  - Check if the CPU supports AVX2 and F16C extensions on Haswell x86 CPUs and newer
 *  - Check if the CPU supports AVX512F and AVX512BW extensions on Skylake x86 CPUs and newer
 *  - Check if the CPU supports AVX512VNNI, AVX512IFMA, AVX512BITALG, AVX512VBMI2, and AVX512VPOPCNTDQ
 *    extensions on Ice Lake x86 CPUs and newer
 *  - Check if the CPU supports AVX512FP16 extensions on Sapphire Rapids x86 CPUs and newer
 *
 *  @return 1 if the CPU supports the SIMD instruction set, 0 otherwise.
 */
SIMSIMD_DYNAMIC int simsimd_uses_neon(void);
SIMSIMD_DYNAMIC int simsimd_uses_sve(void);
SIMSIMD_DYNAMIC int simsimd_uses_haswell(void);
SIMSIMD_DYNAMIC int simsimd_uses_skylake(void);
SIMSIMD_DYNAMIC int simsimd_uses_ice(void);
SIMSIMD_DYNAMIC int simsimd_uses_sapphire(void);
SIMSIMD_DYNAMIC simsimd_capability_t simsimd_capabilities(void);

/*  Inner products
 *  - Dot product: the sum of the products of the corresponding elements of two vectors.
 *  - Complex Dot product: dot product with a conjugate first argument.
 *  - Complex Conjugate Dot product: dot product with a conjugate first argument.
 *
 *  @param a The first vector.
 *  @param b The second vector.
 *  @param n The number of elements in the vectors. Even for complex variants (the number of scalars).
 *  @param d The output distance value.
 *
 *  @note The dot product can be negative, to use as a distance, take `1 - a * b`.
 *  @note The dot product is zero if and only if the two vectors are orthogonal.
 *  @note Defined only for floating-point and integer data types.
 */
SIMSIMD_DYNAMIC void simsimd_dot_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_dot_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_dot_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_dot_f16c(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_dot_f32c(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_dot_f64c(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_vdot_f16c(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                       simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_vdot_f32c(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                       simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_vdot_f64c(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                       simsimd_distance_t* d);

/*  Spatial distances
 *  - Cosine distance: the cosine of the angle between two vectors.
 *  - L2 squared distance: the squared Euclidean distance between two vectors.
 *
 *  @param a The first vector.
 *  @param b The second vector.
 *  @param n The number of elements in the vectors.
 *  @param d The output distance value.
 *
 *  @note The output distance value is non-negative.
 *  @note The output distance value is zero if and only if the two vectors are identical.
 *  @note Defined only for floating-point and integer data types.
 */
SIMSIMD_DYNAMIC void simsimd_cos_i8(simsimd_i8_t const* a, simsimd_i8_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_cos_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_cos_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_cos_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_l2sq_i8(simsimd_i8_t const* a, simsimd_i8_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_l2sq_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_l2sq_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_l2sq_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d);

/*  Binary distances
 *  - Hamming distance: the number of positions at which the corresponding bits are different.
 *  - Jaccard distance: ratio of bit-level matching positions (intersection) to the total number of positions (union).
 *
 *  @param a The first binary vector.
 *  @param b The second binary vector.
 *  @param n The number of 8-bit words in the vectors.
 *  @param d The output distance value.
 *
 *  @note The output distance value is non-negative.
 *  @note The output distance value is zero if and only if the two vectors are identical.
 *  @note Defined only for binary data.
 */
SIMSIMD_DYNAMIC void simsimd_hamming_b8(simsimd_b8_t const* a, simsimd_b8_t const* b, simsimd_size_t n,
                                        simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_jaccard_b8(simsimd_b8_t const* a, simsimd_b8_t const* b, simsimd_size_t n,
                                        simsimd_distance_t* d);

/*  Probability distributions
 *  - Jensen-Shannon divergence: a measure of similarity between two probability distributions.
 *  - Kullback-Leibler divergence: a measure of how one probability distribution diverges from a second.
 *
 *  @param a The first descrete probability distribution.
 *  @param b The second descrete probability distribution.
 *  @param n The number of elements in the descrete distributions.
 *  @param d The output divergence value.
 *
 *  @note The distributions are assumed to be normalized.
 *  @note The output divergence value is non-negative.
 *  @note The output divergence value is zero if and only if the two distributions are identical.
 *  @note Defined only for floating-point data types.
 */
SIMSIMD_DYNAMIC void simsimd_kl_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_kl_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_kl_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_js_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_js_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d);
SIMSIMD_DYNAMIC void simsimd_js_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d);

#else

/*  Compile-time feature-testing functions
 *  - Check if the CPU supports NEON or SVE extensions on Arm
 *  - Check if the CPU supports AVX2 and F16C extensions on Haswell x86 CPUs and newer
 *  - Check if the CPU supports AVX512F and AVX512BW extensions on Skylake x86 CPUs and newer
 *  - Check if the CPU supports AVX512VNNI, AVX512IFMA, AVX512BITALG, AVX512VBMI2, and AVX512VPOPCNTDQ
 *    extensions on Ice Lake x86 CPUs and newer
 *  - Check if the CPU supports AVX512FP16 extensions on Sapphire Rapids x86 CPUs and newer
 *
 *  @return 1 if the CPU supports the SIMD instruction set, 0 otherwise.
 */
SIMSIMD_PUBLIC int simsimd_uses_neon(void) { return SIMSIMD_TARGET_ARM && SIMSIMD_TARGET_NEON; }
SIMSIMD_PUBLIC int simsimd_uses_sve(void) { return SIMSIMD_TARGET_ARM && SIMSIMD_TARGET_SVE; }
SIMSIMD_PUBLIC int simsimd_uses_haswell(void) { return SIMSIMD_TARGET_X86 && SIMSIMD_TARGET_HASWELL; }
SIMSIMD_PUBLIC int simsimd_uses_skylake(void) { return SIMSIMD_TARGET_X86 && SIMSIMD_TARGET_SKYLAKE; }
SIMSIMD_PUBLIC int simsimd_uses_ice(void) { return SIMSIMD_TARGET_X86 && SIMSIMD_TARGET_ICE; }
SIMSIMD_PUBLIC int simsimd_uses_sapphire(void) { return SIMSIMD_TARGET_X86 && SIMSIMD_TARGET_SAPPHIRE; }
SIMSIMD_PUBLIC simsimd_capability_t simsimd_capabilities(void) { return simsimd_capabilities_implementation(); }

/*  Inner products
 *  - Dot product: the sum of the products of the corresponding elements of two vectors.
 *  - Complex Dot product: dot product with a conjugate first argument.
 *  - Complex Conjugate Dot product: dot product with a conjugate first argument.
 *
 *  @param a The first vector.
 *  @param b The second vector.
 *  @param n The number of elements in the vectors. Even for complex variants (the number of scalars).
 *  @param d The output distance value.
 *
 *  @note The dot product can be negative, to use as a distance, take `1 - a * b`.
 *  @note The dot product is zero if and only if the two vectors are orthogonal.
 *  @note Defined only for floating-point and integer data types.
 */
SIMSIMD_PUBLIC void simsimd_dot_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f16_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_dot_f16_neon(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_dot_f16_haswell(a, b, n, d);
#elif SIMSIMD_TARGET_SAPPHIRE
    simsimd_dot_f16_sapphire(a, b, n, d);
#else
    simsimd_dot_f16_serial(a, b, n, d);
#endif
}

SIMSIMD_PUBLIC void simsimd_dot_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f32_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_dot_f32_neon(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_dot_f32_skylake(a, b, n, d);
#else
    simsimd_dot_f32_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_dot_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f64_sve(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_dot_f64_skylake(a, b, n, d);
#else
    simsimd_dot_f64_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_dot_f16c(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f16c_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_dot_f16c_neon(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_dot_f16c_haswell(a, b, n, d);
#elif SIMSIMD_TARGET_sapphire
    simsimd_dot_f16c_sapphire(a, b, n, d);
#else
    simsimd_dot_f16c_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_dot_f32c(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f32c_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_dot_f32c_neon(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_dot_f32c_haswell(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_dot_f32c_skylake(a, b, n, d);
#else
    simsimd_dot_f32c_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_dot_f64c(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f64c_sve(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_dot_f64c_skylake(a, b, n, d);
#else
    simsimd_dot_f64c_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_vdot_f16c(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f16c_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_dot_f16c_neon(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_dot_f16c_haswell(a, b, n, d);
#elif SIMSIMD_TARGET_sapphire
    simsimd_dot_f16c_sapphire(a, b, n, d);
#else
    simsimd_dot_f16c_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_vdot_f32c(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f32c_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_dot_f32c_neon(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_dot_f32c_haswell(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_dot_f32c_skylake(a, b, n, d);
#else
    simsimd_dot_f32c_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_vdot_f64c(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                      simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_dot_f64c_sve(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_dot_f64c_skylake(a, b, n, d);
#else
    simsimd_dot_f64c_serial(a, b, n, d);
#endif
}

/*  Spatial distances
 *  - Cosine distance: the cosine of the angle between two vectors.
 *  - L2 squared distance: the squared Euclidean distance between two vectors.
 *
 *  @param a The first vector.
 *  @param b The second vector.
 *  @param n The number of elements in the vectors.
 *  @param d The output distance value.
 *
 *  @note The output distance value is non-negative.
 *  @note The output distance value is zero if and only if the two vectors are identical.
 *  @note Defined only for floating-point and integer data types.
 */
SIMSIMD_PUBLIC void simsimd_cos_i8(simsimd_i8_t const* a, simsimd_i8_t const* b, simsimd_size_t n,
                                   simsimd_distance_t* d) {
#if SIMSIMD_TARGET_NEON
    simsimd_cos_i8_neon(a, b, n, d);
#elif SIMSIMD_TARGET_ICE
    simsimd_cos_i8_ice(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_cos_i8_haswell(a, b, n, d);
#else
    simsimd_cos_i8_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_l2sq_i8(simsimd_i8_t const* a, simsimd_i8_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d) {
#if SIMSIMD_TARGET_NEON
    simsimd_l2sq_i8_neon(a, b, n, d);
#elif SIMSIMD_TARGET_ICE
    simsimd_l2sq_i8_ice(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_l2sq_i8_haswell(a, b, n, d);
#else
    simsimd_l2sq_i8_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_cos_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_cos_f16_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_cos_f16_neon(a, b, n, d);
#elif SIMSIMD_TARGET_SAPPHIRE
    simsimd_cos_f16_sapphire(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_cos_f16_haswell(a, b, n, d);
#else
    simsimd_cos_f16_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_cos_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_cos_f32_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_cos_f32_neon(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_cos_f32_skylake(a, b, n, d);
#else
    simsimd_cos_f32_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_cos_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                    simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_cos_f64_sve(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_cos_f64_skylake(a, b, n, d);
#else
    simsimd_cos_f64_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_l2sq_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_l2sq_f16_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_l2sq_f16_neon(a, b, n, d);
#elif SIMSIMD_TARGET_sapphire
    simsimd_l2sq_f16_sapphire(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_l2sq_f16_haswell(a, b, n, d);
#else
    simsimd_l2sq_f16_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_l2sq_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_l2sq_f32_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_l2sq_f32_neon(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_l2sq_f32_skylake(a, b, n, d);
#else
    simsimd_l2sq_f32_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_l2sq_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                     simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_l2sq_f64_sve(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_l2sq_f64_skylake(a, b, n, d);
#else
    simsimd_l2sq_f64_serial(a, b, n, d);
#endif
}

/*  Binary distances
 *  - Hamming distance: the number of positions at which the corresponding bits are different.
 *  - Jaccard distance: ratio of bit-level matching positions (intersection) to the total number of positions (union).
 *
 *  @param a The first binary vector.
 *  @param b The second binary vector.
 *  @param n The number of 8-bit words in the vectors.
 *  @param d The output distance value.
 *
 *  @note The output distance value is non-negative.
 *  @note The output distance value is zero if and only if the two vectors are identical.
 *  @note Defined only for binary data.
 */
SIMSIMD_PUBLIC void simsimd_hamming_b8(simsimd_b8_t const* a, simsimd_b8_t const* b, simsimd_size_t n,
                                       simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_hamming_b8_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_hamming_b8_neon(a, b, n, d);
#elif SIMSIMD_TARGET_ICE
    simsimd_hamming_b8_ice(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_hamming_b8_haswell(a, b, n, d);
#else
    simsimd_hamming_b8_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_jaccard_b8(simsimd_b8_t const* a, simsimd_b8_t const* b, simsimd_size_t n,
                                       simsimd_distance_t* d) {
#if SIMSIMD_TARGET_SVE
    simsimd_jaccard_b8_sve(a, b, n, d);
#elif SIMSIMD_TARGET_NEON
    simsimd_jaccard_b8_neon(a, b, n, d);
#elif SIMSIMD_TARGET_ICE
    simsimd_jaccard_b8_ice(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_jaccard_b8_haswell(a, b, n, d);
#else
    simsimd_jaccard_b8_serial(a, b, n, d);
#endif
}

/*  Probability distributions
 *  - Jensen-Shannon divergence: a measure of similarity between two probability distributions.
 *  - Kullback-Leibler divergence: a measure of how one probability distribution diverges from a second.
 *
 *  @param a The first descrete probability distribution.
 *  @param b The second descrete probability distribution.
 *  @param n The number of elements in the descrete distributions.
 *  @param d The output divergence value.
 *
 *  @note The distributions are assumed to be normalized.
 *  @note The output divergence value is non-negative.
 *  @note The output divergence value is zero if and only if the two distributions are identical.
 *  @note Defined only for floating-point data types.
 */
SIMSIMD_PUBLIC void simsimd_kl_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                   simsimd_distance_t* d) {
#if SIMSIMD_TARGET_NEON
    simsimd_kl_f16_neon(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_kl_f16_haswell(a, b, n, d);
#else
    simsimd_kl_f16_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_kl_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                   simsimd_distance_t* d) {
#if SIMSIMD_TARGET_NEON
    simsimd_kl_f32_neon(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_kl_f32_skylake(a, b, n, d);
#else
    simsimd_kl_f32_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_kl_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                   simsimd_distance_t* d) {
    simsimd_kl_f64_serial(a, b, n, d);
}
SIMSIMD_PUBLIC void simsimd_js_f16(simsimd_f16_t const* a, simsimd_f16_t const* b, simsimd_size_t n,
                                   simsimd_distance_t* d) {
#if SIMSIMD_TARGET_NEON
    simsimd_js_f16_neon(a, b, n, d);
#elif SIMSIMD_TARGET_HASWELL
    simsimd_js_f16_haswell(a, b, n, d);
#else
    simsimd_js_f16_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_js_f32(simsimd_f32_t const* a, simsimd_f32_t const* b, simsimd_size_t n,
                                   simsimd_distance_t* d) {
#if SIMSIMD_TARGET_NEON
    simsimd_js_f32_neon(a, b, n, d);
#elif SIMSIMD_TARGET_SKYLAKE
    simsimd_js_f32_skylake(a, b, n, d);
#else
    simsimd_js_f32_serial(a, b, n, d);
#endif
}
SIMSIMD_PUBLIC void simsimd_js_f64(simsimd_f64_t const* a, simsimd_f64_t const* b, simsimd_size_t n,
                                   simsimd_distance_t* d) {
    simsimd_js_f64_serial(a, b, n, d);
}

#endif

#ifdef __cplusplus
}
#endif

#endif // SIMSIMD_H
