#pragma once

#include <vector>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include <simsimd/simsimd.h>

using namespace kuzu::common;

namespace kuzu {
namespace storage {
// TODO: Probably get rid of virtual functions and use template specialization. Benchmark it!!

enum DistanceType {
    L2_SQ,
    INNER_PRODUCT,
    COSINE,
};

template<typename T1, typename T2>
struct DC {
    /**
     * Compute the distance between two vectors.
     * @param x the first vector
     * @param y the second vector
     * @param result the distance
     */
    virtual void compute_distance(const T1* x, const T2* y, double* result) = 0;

    /**
     * Compute the asymmetric batch distances between n vectors. More accurate but slower.
     * @param x the first set of vectors
     * @param y the second set of vectors
     * @param result the distances
     * @param n the number of vectors
     */
    virtual void batch_compute_distances(const T1* x, const T2* y, double* results, size_t n) = 0;

    virtual ~DC() = default;
};

template<typename T>
struct Quantizer {
    explicit Quantizer(int dim = 0, size_t codeSize = 0) : dim(dim), codeSize(codeSize){};

    /**
     * Train the quantizer
     * @param n number of training vectors
     * @param x training vectors, size n * d
     */
    virtual void batch_train(size_t n, const float* x) = 0;

    /**
     * Quantize a set of vectors
     * @param x input vectors, size n * d
     * @param codes output codes, size n * code_size
     */
    virtual void encode(const float* x, T* codes, size_t n) const = 0;

    /**
     * Decode a set of vectors
     * @param code input codes, size n * code_size
     * @param x output vectors, size n * d
     */
    virtual void decode(const T* code, float* x, size_t n) const = 0;

    virtual void serialize(Serializer& serializer) const = 0;

    /**
     * Get the asymmetric distance computer which computes the distance between a quantized
     * vector and a actual float vector. This is more accurate but slower.
     */
    virtual std::unique_ptr<DC<float, T>> get_asym_distance_computer(DistanceType type) const = 0;

    /**
     * Get the symmetric distance computer which computes the distance between two quantized
     * vectors. This is faster.
     */
    virtual std::unique_ptr<DC<T, T>> get_sym_distance_computer(DistanceType type) const = 0;

    virtual ~Quantizer() = default;

    int dim;         // dimension of the input vectors
    size_t codeSize; // bytes per indexed vector
};

// Scalar quantization for 8-bit integers
// Encoding:
// 1. Scale to [0, 1] => (x - vmin) / vdiff
// 2. Scale to [0, 255] => x * 255
// 3. Round to the nearest integer
//
// Decoding:
// 1. Scale to [0, 1] => (ci + 0.5) / 255
// 2. Scale to [vmin, vmax] => vmin + ci * vdiff
//
// Reason to add 0.5f is to round the value:
// This is used for continuity correction. Basically the probability that a random variable
// falls in a certain range is the same as the probability that the random variable falls in the
// corresponding integer value. It gives out a more accurate result.
//
// Use precomputed values to calculate the distance between two encoded vectors:
// distance = i8 * i8' * alpha^2 + i8 * beta * alpha + i8' * beta * alpha + beta^2
// where alpha = vdiff / 255.0f and beta = 0.5f * alpha + vmin
//
// We can precompute alpha^2 and gamma^2 to speed up the computation. Additionally, we can
// precompute i8 * alpha * gamma for each vector and store it as part of the compressed data. This
// will improve distance computation speed. Therefore, we need 4 more additional bytes per vector to
// store these precomputed values.
//
// Additionally, decoding can also be written as:
// x = alpha * ci + beta
// TODO: Think about making precomputed values optional

inline uint8_t encode_serial(const float* x, size_t i, const float* vmin, const float* vdiff) {
    return int(((x[i] - vmin[i]) / vdiff[i]) * 255.0f);
}

inline void encode_serial(const float* x, uint8_t* codes, size_t n, int dim, const float* vmin,
    const float* vdiff, const float* alpha, const float* beta) {
    for (size_t i = 0; i < n; i++) {
        auto* xi = x + i * dim;
        // We need to skip the last 4 bytes as they are precomputed values
        auto* ci = codes + i * (dim + 4);
        float precompute_value = 0;
        for (size_t j = 0; j < dim; j++) {
            ci[j] = encode_serial(xi, j, vmin, vdiff);
            precompute_value += ci[j] * alpha[j] * beta[j];
        }
        // Store precomputed values
        *reinterpret_cast<float*>(ci + dim) = precompute_value;
    }
}

inline float decode_serial(const uint8_t* code, size_t i, const float* alpha, const float* beta) {
    return alpha[i] * code[i] + beta[i];
}

inline void decode_serial(const uint8_t* code, float* x, size_t n, int dim, const float* alpha,
    const float* beta) {
    for (size_t i = 0; i < n; i++) {
        // We need to skip the last 4 bytes as they are precomputed values
        auto* ci = code + i * (dim + 4);
        auto* xi = x + i * dim;
        for (size_t j = 0; j < dim; j++) {
            xi[j] = decode_serial(ci, j, alpha, beta);
        }
    }
}

// This doesn't use precomputed values
inline void compute_asym_ip_serial(const float* x, const uint8_t* y, double* result, size_t dim,
    const float* alpha, const float* beta) {
    auto* decoded = new float[dim];
    decode_serial(y, decoded, 1, dim, alpha, beta);
    simsimd_dot_f32_serial(x, decoded, dim, result);
}

// This uses precomputed values
inline void compute_sym_ip_serial(const uint8_t* x, const uint8_t* y, double* result, size_t dim,
    const float* alphaSqr, const float* betaSqr) {
    double xy = 0;
    for (size_t i = 0; i < dim; i++) {
        xy += x[i] * y[i] * alphaSqr[i] + betaSqr[i];
    }
    // Add precomputed value (last 4 bytes)
    xy += *reinterpret_cast<const float*>(x + dim);
    *result = xy;
}

inline void compute_asym_l2sq_serial(const float* x, const uint8_t* y, double* result, size_t dim,
    const float* alpha, const float* beta) {
    auto* decoded = new float[dim];
    decode_serial(y, decoded, 1, dim, alpha, beta);
    simsimd_l2sq_f32(x, decoded, dim, result);
}

inline void compute_asym_cos_serial(const float* x, const uint8_t* y, double* result, size_t dim,
    const float* alpha, const float* beta) {
    auto* decoded = new float[dim];
    decode_serial(y, decoded, 1, dim, alpha, beta);
    simsimd_cos_f32(x, decoded, dim, result);
}

#if SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_NEON
#pragma GCC push_options
#pragma GCC target("+simd")
#pragma clang attribute push(__attribute__((target("+simd"))), apply_to = function)

inline uint8x8_t encode_neon(const float* x, size_t i, const float* vmin, const float* vdiff) {
    // Faster version
    // 255 => 255.0f
    float32x4_t const_255 = vdupq_n_f32(255.0f);

    float32x4_t x_low = vld1q_f32(x + i);
    float32x4_t x_high = vld1q_f32(x + i + 4);

    // Scale to [0, 1] => (x - vmin) / vdiff
    float32x4_t x_scaled_low =
        vdivq_f32(vsubq_f32(x_low, vld1q_f32(vmin + i)), vld1q_f32(vdiff + i));
    float32x4_t x_scaled_high =
        vdivq_f32(vsubq_f32(x_high, vld1q_f32(vmin + i + 4)), vld1q_f32(vdiff + i + 4));

    // Scale to [0, 255] => x * 255
    // TODO: What if we use vmulq_f32(x_scaled_low, const_255) to calculate precomputed values? Will
    // it
    //  increase accuracy?
    uint32x4_t ci_low = vcvtq_u32_f32(vmulq_f32(x_scaled_low, const_255));
    uint32x4_t ci_high = vcvtq_u32_f32(vmulq_f32(x_scaled_high, const_255));

    // Clamp to [0, 255]
    return vmovn_u16(vcombine_u16(vmovn_u32(ci_low), vmovn_u32(ci_high)));
}

inline float32x4_t calc_precomputed_values_neon(uint8x8_t ci_vec, size_t i, const float* alpha,
    const float* beta) {
    uint16x8_t ci_vec16 = vmovl_u8(ci_vec);
    float32x4_t ci_vec32_low = vcvtq_f32_u32(vmovl_u16(vget_low_u16(ci_vec16)));
    float32x4_t ci_vec32_high = vcvtq_f32_u32(vmovl_u16(vget_high_u16(ci_vec16)));

    // Calculate precomputed values i8 * alpha * beta
    float32x4_t precompute_values_low =
        vmulq_f32(vmulq_f32(ci_vec32_low, vld1q_f32(alpha + i)), vld1q_f32(beta + i));
    float32x4_t precompute_values_high =
        vmulq_f32(vmulq_f32(ci_vec32_high, vld1q_f32(alpha + i + 4)), vld1q_f32(beta + i + 4));

    return vaddq_f32(precompute_values_low, precompute_values_high);
}

inline void encode_neon(const float* x, uint8_t* codes, size_t n, int dim, const float* vmin,
    const float* vdiff, const float* alpha, const float* beta) {
    for (size_t i = 0; i < n; i++) {
        size_t j = 0;
        const float* xi = x + i * dim;
        // We need to skip the last 4 bytes as they are precomputed values
        uint8_t* ci = codes + i * (dim + 4);
        float32x4_t precompute_values = vdupq_n_f32(0);
        for (; j + 8 <= dim; j += 8) {
            uint8x8_t ci_vec = encode_neon(xi, j, vmin, vdiff);
            precompute_values =
                vaddq_f32(calc_precomputed_values_neon(ci_vec, j, alpha, beta), precompute_values);
            vst1_u8(ci + j, ci_vec);
        }
        // Handle the remaining
        for (; j < dim; j++) {
            ci[j] = encode_serial(xi, j, vmin, vdiff);
        }
        // Store precomputed values
        *reinterpret_cast<float*>(ci + dim) = vaddvq_f32(precompute_values);
    }
}

inline float32x4x2_t decode_neon(const uint8_t* code, size_t i, const float* alpha,
    const float* beta) {
    uint8x8_t ci_vec = vld1_u8(code + i);
    uint16x8_t ci_vec16 = vmovl_u8(ci_vec);
    float32x4_t ci_vec32_low = vcvtq_f32_u32(vmovl_u16(vget_low_u16(ci_vec16)));
    float32x4_t ci_vec32_high = vcvtq_f32_u32(vmovl_u16(vget_high_u16(ci_vec16)));

    // x = alpha * ci + beta
    float32x4_t x_low = vmlaq_f32(vld1q_f32(beta + i), vld1q_f32(alpha + i), ci_vec32_low);
    float32x4_t x_high = vmlaq_f32(vld1q_f32(beta + i + 4), vld1q_f32(alpha + i + 4), ci_vec32_high);
    return {x_low, x_high};
}

inline void decode_neon(const uint8_t* code, float* x, size_t n, int dim, const float* alpha,
    const float* beta) {
    for (size_t i = 0; i < n; i++) {
        size_t j = 0;
        // We need to skip the last 4 bytes as they are precomputed values
        const uint8_t* ci = code + i * (dim + 4);
        float* xi = x + i * dim;
        for (; j + 8 <= dim; j += 8) {
            float32x4x2_t x0 = decode_neon(ci, j, alpha, beta);
            vst1q_f32(xi + j, x0.val[0]);
            vst1q_f32(xi + j + 4, x0.val[1]);
        }
        // Handle the remaining
        for (; j < dim; j++) {
            xi[j] = decode_serial(ci, j, alpha, beta);
        }
    }
}

// This doesn't use precomputed values
inline void compute_asym_ip_neon(const float* x, const uint8_t* y, double* result, size_t dim,
    const float* alpha, const float* beta) {
    float32x4_t xy_vec = vdupq_n_f32(0);
    size_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        float32x4x2_t y_decoded = decode_neon(y, i, alpha, beta);
        float32x4_t x_low_vec = vld1q_f32(x + i);
        float32x4_t y_low_vec = y_decoded.val[0];
        float32x4_t x_high_vec = vld1q_f32(x + i + 4);
        float32x4_t y_high_vec = y_decoded.val[1];

        xy_vec = vmlaq_f32(xy_vec, x_low_vec, y_low_vec);
        xy_vec = vmlaq_f32(xy_vec, x_high_vec, y_high_vec);
    }
    float xy = vaddvq_f32(xy_vec);

    // Calculate the remaining. See if vectorization is possible or will be helpful
    for (; i < dim; i++) {
        float xi = x[i];
        float yi = decode_serial(y, i, alpha, beta);
        xy += xi * yi;
    }
    *result = xy;
}

// This uses precomputed values
inline void compute_sym_ip_neon(const uint8_t* x, const uint8_t* y, double* result, size_t dim,
    const float* alphaSqr, const float* betaSqr) {
    size_t i = 0;
    float32x4_t xy_vec = vdupq_n_f32(0);
    for (; i + 8 < dim; i += 8) {
        uint16x8_t x0 = vmovl_u8(vld1_u8(x + i));
        uint16x8_t y0 = vmovl_u8(vld1_u8(y + i));

        float32x4_t x0_low = vcvtq_f32_u32(vmovl_u16(vget_low_u16(x0)));
        float32x4_t x0_high = vcvtq_f32_u32(vmovl_u16(vget_high_u16(x0)));
        float32x4_t y0_low = vcvtq_f32_u32(vmovl_u16(vget_low_u16(y0)));
        float32x4_t y0_high = vcvtq_f32_u32(vmovl_u16(vget_high_u16(y0)));

        float32x4_t alphaSqr_low = vld1q_f32(alphaSqr + i);
        float32x4_t alphaSqr_high = vld1q_f32(alphaSqr + i + 4);

        float32x4_t gammaSqr_low = vld1q_f32(betaSqr + i);
        float32x4_t gammaSqr_high = vld1q_f32(betaSqr + i + 4);

        float32x4_t xy_low = vfmaq_f32(gammaSqr_low, vmulq_f32(x0_low, y0_low), alphaSqr_low);
        float32x4_t xy_high = vfmaq_f32(gammaSqr_high, vmulq_f32(x0_high, y0_high), alphaSqr_high);
        xy_vec = vaddq_f32(xy_vec, xy_low);
        xy_vec = vaddq_f32(xy_vec, xy_high);
    }
    float xy = vaddvq_f32(xy_vec);

    // Calculate the remaining
    for (; i < dim; i++) {
        xy += x[i] * y[i] * alphaSqr[i] + betaSqr[i];
    }

    // Add precomputed value (last 4 bytes)
    xy += *reinterpret_cast<const float*>(x + dim);
    *result = xy;
}

inline void compute_asym_cos_neon(const float* x, const uint8_t* y, double* result, size_t dim,
    const float* alpha, const float* beta) {
    float32x4_t xy_vec = vdupq_n_f32(0), x2_vec = vdupq_n_f32(0), y2_vec = vdupq_n_f32(0);
    simsimd_size_t i = 0;
    for (; i + 8 <= dim; i += 8) {
        float32x4x2_t y_decoded = decode_neon(y, i, alpha, beta);
        float32x4_t x_low_vec = vld1q_f32(x + i);
        float32x4_t y_low_vec = y_decoded.val[0];
        float32x4_t x_high_vec = vld1q_f32(x + i + 4);
        float32x4_t y_high_vec = y_decoded.val[1];

        xy_vec = vfmaq_f32(xy_vec, x_low_vec, y_low_vec);
        xy_vec = vfmaq_f32(xy_vec, x_high_vec, y_high_vec);
        x2_vec = vfmaq_f32(x2_vec, x_low_vec, x_low_vec);
        x2_vec = vfmaq_f32(x2_vec, x_high_vec, x_high_vec);
        y2_vec = vfmaq_f32(y2_vec, y_low_vec, y_low_vec);
        y2_vec = vfmaq_f32(y2_vec, y_high_vec, y_high_vec);
    }

    simsimd_f32_t xy = vaddvq_f32(xy_vec), x2 = vaddvq_f32(x2_vec), y2 = vaddvq_f32(y2_vec);
    for (; i < dim; ++i) {
        float xi = x[i], yi = decode_serial(y, i, alpha, beta);
        xy += xi * yi;
        x2 += xi * xi;
        y2 += yi * yi;
    }

    simsimd_f32_t x2_y2_arr[2] = {x2, y2};
    vst1_f32(x2_y2_arr, vrsqrte_f32(vld1_f32(x2_y2_arr)));
    *result = xy != 0 ? 1 - xy * x2_y2_arr[0] * x2_y2_arr[1] : 1;
}

#pragma clang attribute pop
#pragma GCC pop_options
#endif
#endif

#if SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_HASWELL

#pragma GCC push_options
#pragma GCC target("avx2", "fma")
#pragma clang attribute push(__attribute__((target("avx2,fma"))), apply_to = function)

inline __m128i encode_haswell(const float* x, size_t i, const float* vmin, const float* vdiff) {
    // Faster version
    // 255 => 255.0f
    __m256 const_255 = _mm256_set1_ps(255.0f);

    // Load the input values
    __m256 x_values = _mm256_loadu_ps(x + i);

    // Load the vmin and vdiff values
    __m256 vmin_values = _mm256_loadu_ps(vmin + i);
    __m256 vdiff_values = _mm256_loadu_ps(vdiff + i);

    // Scale to [0, 1] => (x - vmin) / vdiff
    __m256 x_scaled = _mm256_div_ps(_mm256_sub_ps(x_values, vmin_values), vdiff_values);

    // Scale to [0, 255] => x * 255
    __m256 x_scaled_255 = _mm256_mul_ps(x_scaled, const_255);

    // Convert to integers
    __m256i ci = _mm256_cvtps_epi32(x_scaled_255);

    // Pack and clamp to [0, 255]
    __m128i ci_low = _mm256_extracti128_si256(ci, 0);
    __m128i ci_high = _mm256_extracti128_si256(ci, 1);

    // Saturate to uint8
    __m128i result = _mm_packus_epi32(ci_low, ci_high);

    return result;
}

inline __m256 calc_precomputed_values_haswell_2(__m128i ci_vec, size_t i, const float* alpha,
    const float* beta) {
    // Unpack the 8-bit integers to 32-bit integers
    __m256i ci_vec32 = _mm256_cvtepu8_epi32(ci_vec);

    // Convert to float
    __m256 ci_vec_float = _mm256_cvtepi32_ps(ci_vec32);

    // Load alpha and beta
    __m256 alpha_values = _mm256_loadu_ps(alpha + i);
    __m256 beta_values = _mm256_loadu_ps(beta + i);

    // Calculate precomputed values: i8 * alpha * beta
    __m256 precompute_values =
        _mm256_mul_ps(_mm256_mul_ps(ci_vec_float, alpha_values), beta_values);

    return precompute_values;
}

inline __m256 calc_precomputed_values_haswell(__m128i ci_vec, size_t i, const float* alphaSqr) {
    // Unpack the 8-bit integers to 32-bit integers
    __m256i ci_vec32 = _mm256_cvtepu8_epi32(ci_vec);

    // Convert to float
    __m256 ci_vec_float = _mm256_cvtepi32_ps(ci_vec32);

    // Load alpha and beta
    __m256 alpha_sqr_values = _mm256_loadu_ps(alphaSqr + i);

    // Calculate precomputed values: i8 * alpha * beta
    __m256 precompute_values =
        _mm256_mul_ps(_mm256_mul_ps(ci_vec_float, ci_vec_float), alpha_sqr_values);

    return precompute_values;
}

inline void encode_haswell(const float* x, uint8_t* codes, size_t n, int dim, const float* vmin,
    const float* vdiff, const float* alphaSqr) {
    for (size_t i = 0; i < n; i++) {
        size_t j = 0;
        const float* xi = x + i * dim;
        // We need to skip the last 4 bytes as they are precomputed values
        uint8_t* ci = codes + i * (dim + 4);
        __m256 precompute_values = _mm256_setzero_ps();
        for (; j + 8 <= dim; j += 8) {
            __m128i ci_vec = encode_haswell(xi, j, vmin, vdiff);
            precompute_values = _mm256_add_ps(calc_precomputed_values_haswell(ci_vec, j, alphaSqr),
                precompute_values);
            _mm_storeu_si128((__m128i*)(ci + j), ci_vec);
        }
        // Handle the remaining
        for (; j < dim; j++) {
            ci[j] = encode_serial(xi, j, vmin,
                vdiff); // Ensure encode_serial is available and works for single elements
        }
        // Store precomputed values
        *reinterpret_cast<float*>(ci + dim) =
            _mm_cvtss_f32(_mm256_castps256_ps128(precompute_values));
    }
}

inline __m256 decode_haswell(const uint8_t* code, size_t i, const float* alpha, const float* beta) {
    __m128i ci_vec = _mm_loadu_si128((__m128i const*)(code + i));
    __m256i ci_vec32 = _mm256_cvtepu8_epi32(ci_vec);
    __m256 ci_vec_float = _mm256_cvtepi32_ps(ci_vec32);

    // Load alpha and beta
    __m256 alpha_values = _mm256_loadu_ps(alpha + i);
    __m256 beta_values = _mm256_loadu_ps(beta + i);

    // x = alpha * ci + beta
    __m256 x_values = _mm256_fmadd_ps(alpha_values, ci_vec_float, beta_values);

    return x_values;
}

inline void decode_haswell(const uint8_t* code, float* x, size_t n, int dim, const float* alpha,
    const float* beta) {
    for (size_t i = 0; i < n; i++) {
        size_t j = 0;
        // We need to skip the last 4 bytes as they are precomputed values
        const uint8_t* ci = code + i * (dim + 4);
        float* xi = x + i * dim;
        for (; j + 8 <= dim; j += 8) {
            __m256 x_values = decode_haswell(ci, j, alpha, beta);
            _mm256_storeu_ps(xi + j, x_values);
        }
        // Handle the remaining
        for (; j < dim; j++) {
            xi[j] = decode_serial(ci, j, alpha,
                beta); // Ensure decode_serial is available and works for single elements
        }
    }
}

#pragma clang attribute pop
#pragma GCC pop_options
#endif // SIMSIMD_TARGET_HASWELL

#if SIMSIMD_TARGET_SKYLAKE
#pragma GCC push_options
#pragma GCC target("avx512f", "avx512vl", "bmi2")
#pragma clang attribute push(__attribute__((target("avx512f,avx512vl,bmi2"))), apply_to = function)

inline __m512 decode_skylake(const uint8_t* code, size_t i, const float* alpha, const float* beta) {
    __m128i ci_vec = _mm_loadu_si128((__m128i const*)(code + i));
    __m512i ci_vec32 = _mm512_cvtepu8_epi32(ci_vec);
    __m512 ci_vec_float = _mm512_cvtepi32_ps(ci_vec32);

    // Load alpha and beta
    __m512 alpha_values = _mm512_loadu_ps(alpha + i);
    __m512 beta_values = _mm512_loadu_ps(beta + i);

    // x = alpha * ci + beta
    __m512 x_values = _mm512_fmadd_ps(alpha_values, ci_vec_float, beta_values);

    return x_values;
}

inline void compute_asym_l2sq_skylake(const float* x, const uint8_t* y, double* result, size_t dim,
    const float* alpha, const float* beta) {
    __m512 d2_vec = _mm512_setzero();
    __m512 a_vec, b_vec;
    size_t j = 0;
    for (; j + 16 <= dim; j += 16) {
        a_vec = decode_skylake(y, j, alpha, beta);
        b_vec = _mm512_loadu_ps(x + j);
        __m512 d_vec = _mm512_sub_ps(a_vec, b_vec);
        d2_vec = _mm512_fmadd_ps(d_vec, d_vec, d2_vec);
    }
    *result = _mm512_reduce_add_ps(d2_vec);
}

inline void compute_asym_cos_skylake(const float* x, const uint8_t* y, double* result, size_t dim,
    const float* alpha, const float* beta) {
    __m512 ab_vec = _mm512_setzero();
    __m512 a2_vec = _mm512_setzero();
    __m512 b2_vec = _mm512_setzero();
    __m512 a_vec, b_vec;
    size_t j = 0;
    for (; j + 16 <= dim; j += 16) {
        a_vec = decode_skylake(y, j, alpha, beta);
        b_vec = _mm512_loadu_ps(x + j);
        ab_vec = _mm512_fmadd_ps(a_vec, b_vec, ab_vec);
        a2_vec = _mm512_fmadd_ps(a_vec, a_vec, a2_vec);
        b2_vec = _mm512_fmadd_ps(b_vec, b_vec, b2_vec);
    }

    simsimd_f32_t ab = _mm512_reduce_add_ps(ab_vec);
    simsimd_f32_t a2 = _mm512_reduce_add_ps(a2_vec);
    simsimd_f32_t b2 = _mm512_reduce_add_ps(b2_vec);

    // Compute the reciprocal square roots of a2 and b2
    // Mysteriously, MSVC has no `_mm_rsqrt14_ps` intrinsic, but has it's masked variants,
    // so let's use `_mm_maskz_rsqrt14_ps(0xFF, ...)` instead.
    __m128 rsqrts = _mm_maskz_rsqrt14_ps(0xFF, _mm_set_ps(0.f, 0.f, a2 + 1.e-9f, b2 + 1.e-9f));
    simsimd_f32_t rsqrt_a2 = _mm_cvtss_f32(rsqrts);
    simsimd_f32_t rsqrt_b2 = _mm_cvtss_f32(_mm_shuffle_ps(rsqrts, rsqrts, _MM_SHUFFLE(0, 0, 0, 1)));
    *result = 1 - ab * rsqrt_a2 * rsqrt_b2;
}

#pragma clang attribute pop
#pragma GCC pop_options
#endif // SIMSIMD_TARGET_SKYLAKE
#endif // SIMSIMD_TARGET_X86

class AsymmetricCosine : public DC<float, uint8_t> {
public:
    explicit AsymmetricCosine(int dim, const float* alpha, const float* beta)
        : dim(dim), alpha(alpha), beta(beta){};

    ~AsymmetricCosine() = default;

    inline void compute_distance(const float* x, const uint8_t* y, double* result) override {
#if SIMSIMD_TARGET_NEON
        compute_asym_cos_neon(x, y, result, dim, alpha, beta);
#elif SIMSIMD_TARGET_SKYLAKE
        compute_asym_cos_skylake(x, y, result, dim, alpha, beta);
#else
        compute_asym_cos_serial(x, y, result, dim, alpha, beta);
#endif
    }

    inline void batch_compute_distances(const float* x, const uint8_t* y, double* results,
        size_t n) override {
        for (size_t i = 0; i < n; i++) {
            // We need to skip the last 4 bytes as they are precomputed values
            compute_distance(x + i * dim, y + i * (dim + 4), results + i);
        }
    }

private:
    int dim;
    const float* alpha;
    const float* beta;
};

// This doesn't use precomputed values
class AsymmetricIP : public DC<float, uint8_t> {
public:
    explicit AsymmetricIP(int dim, const float* alpha, const float* beta)
        : dim(dim), alpha(alpha), beta(beta){};

    ~AsymmetricIP() = default;

    inline void compute_distance(const float* x, const uint8_t* y, double* result) override {
#if SIMSIMD_TARGET_NEON
        compute_asym_ip_neon(x, y, result, dim, alpha, beta);
#else
        compute_asym_ip_serial(x, y, result, dim, alpha, beta);
#endif
    }

    inline void batch_compute_distances(const float* x, const uint8_t* y, double* results,
        size_t n) override {
        for (size_t i = 0; i < n; i++) {
            // We need to skip the last 4 bytes as they are precomputed values
            compute_distance(x + i * dim, y + i * (dim + 4), results + i);
        }
    }

private:
    int dim;
    const float* alpha;
    const float* beta;
};

// This uses precomputed values
class SymmetricIP : public DC<uint8_t, uint8_t> {
public:
    explicit SymmetricIP(int dim, const float* alphaSqr, const float* betaSqr)
        : dim(dim), alphaSqr(alphaSqr), betaSqr(betaSqr){};

    ~SymmetricIP() = default;

    inline void compute_distance(const uint8_t* x, const uint8_t* y, double* result) override {
#if SIMSIMD_TARGET_NEON
        compute_sym_ip_neon(x, y, result, dim, alphaSqr, betaSqr);
#else
        compute_sym_ip_serial(x, y, result, dim, alphaSqr, betaSqr);
#endif
    }

    inline void batch_compute_distances(const uint8_t* x, const uint8_t* y, double* results,
        size_t n) override {
        for (size_t i = 0; i < n; i++) {
            // We need to skip the last 4 bytes as they are precomputed values
            compute_distance(x + i * (dim + 4), y + i * (dim + 4), results + i);
        }
    }

private:
    int dim;
    const float* alphaSqr;
    const float* betaSqr;
};

class AsymmetricL2Sq : public DC<float, uint8_t> {
public:
    explicit AsymmetricL2Sq(int dim, const float* alpha, const float* beta)
        : dim(dim), alpha(alpha), beta(beta){};

    ~AsymmetricL2Sq() = default;

    inline void compute_distance(const float* x, const uint8_t* y, double* result) override {
#if SIMSIMD_TARGET_SKYLAKE
        compute_asym_l2sq_skylake(x, y, result, dim, alpha, beta);
#else
        compute_asym_l2sq_serial(x, y, result, dim, alpha, beta);
#endif
    }

    inline void batch_compute_distances(const float* x, const uint8_t* y, double* results,
        size_t n) override {
        for (size_t i = 0; i < n; i++) {
            // We need to skip the last 4 bytes as they are precomputed values
            compute_distance(x + i * dim, y + i * (dim + 4), results + i);
        }
    }

private:
    int dim;
    const float* alpha;
    const float* beta;
};

class SQ8Bit : public Quantizer<uint8_t> {
    static constexpr size_t NUM_BINS = 512;
    static constexpr float BREAK_POINT_DATA_RATIO = 0.95f;
public:
    explicit SQ8Bit(int dim) : Quantizer(dim, dim + 4) {
        vmin = new float[dim];
        vdiff = new float[dim];
        for (size_t i = 0; i < dim; i++) {
            vmin[i] = std::numeric_limits<float>::max();
            vdiff[i] = std::numeric_limits<float>::lowest();
        }

        alpha = new float[dim];
        beta = new float[dim];
        alphaSqr = new float[dim];
        betaSqr = new float[dim];
    }

    // Copy constructor
    explicit SQ8Bit(const SQ8Bit& other) : Quantizer(other.dim, other.codeSize) {
        vmin = new float[dim];
        vdiff = new float[dim];
        alpha = new float[dim];
        beta = new float[dim];
        alphaSqr = new float[dim];
        betaSqr = new float[dim];

        memcpy(vmin, other.vmin, dim * sizeof(float));
        memcpy(vdiff, other.vdiff, dim * sizeof(float));
        memcpy(alpha, other.alpha, dim * sizeof(float));
        memcpy(beta, other.beta, dim * sizeof(float));
        memcpy(alphaSqr, other.alphaSqr, dim * sizeof(float));
        memcpy(betaSqr, other.betaSqr, dim * sizeof(float));
    }

    inline void determine_smallest_breakpoint(size_t n, const float *data) {
        // Use histogram to determine the smallest break point.
        // We will use 256 bins.
        std::vector<std::vector<uint64_t>> histogram(dim);
        for (size_t i = 0; i < dim; i++) {
            histogram[i] = std::vector<uint64_t>(NUM_BINS, 0);
        }

        for (size_t i = 0; i < n; i++) {
            for (size_t j = 0; j < dim; j++) {
                // Determine the bin using vmin and vdiff.
                auto bin = static_cast<uint64_t>(((data[i * dim + j] - vmin[j]) / vdiff[j]) * NUM_BINS);
                bin = std::min(bin, (uint64_t) (NUM_BINS - 1));
                if (bin >= NUM_BINS) {
                    printf("bin: %d, data: %f, vmin: %f, vdiff: %f\n", bin, data[i * dim + j], vmin[j], vdiff[j]);
                }
                histogram[j][bin]++;
            }
        }

        // Now we have to find the smallest which contains at-least 70% of n of the data.
        // Find the smallest bin range that contains at least 70% of the data
        auto threshold = n * BREAK_POINT_DATA_RATIO;
        for (size_t i = 0; i < dim; i++) {
            size_t start_bin = 0;
            size_t end_bin = 0;
            size_t min_bin_size = NUM_BINS;
            size_t sum = 0;
            size_t left = 0;

            // Sliding window approach to find the smallest range
            for (size_t right = 0; right < NUM_BINS; right++) {
                sum += histogram[i][right];

                // Shrink the window from the left if the threshold is met
                while (sum >= threshold) {
                    if (right - left < min_bin_size) {
                        min_bin_size = right - left;
                        start_bin = left;
                        end_bin = right;
                    }
                    sum -= histogram[i][left];
                    left++;
                }
            }
            vmin[i] = vmin[i] + (float) start_bin / NUM_BINS * vdiff[i];
            vdiff[i] = (float) (end_bin - start_bin) / NUM_BINS * vdiff[i];
        }
    }

    inline void batch_train(size_t n, const float* x) override {
        for (size_t i = 0; i < n; i++) {
            for (size_t j = 0; j < dim; j++) {
                vmin[j] = std::min(vmin[j], x[i * dim + j]);
                vdiff[j] = std::max(vdiff[j], x[i * dim + j]);
            }
        }

        for (size_t i = 0; i < dim; i++) {
            vdiff[i] -= vmin[i];
        }

        determine_smallest_breakpoint(n, x);

        // Precompute alpha and gamma, as well as their squares
        for (size_t i = 0; i < dim; i++) {
            alpha[i] = vdiff[i] / 255.0f;
            beta[i] = (0.5f * alpha[i]) + vmin[i];
            alphaSqr[i] = alpha[i] * alpha[i];
            betaSqr[i] = beta[i] * beta[i];
        }
    }

    inline void encode(const float* x, uint8_t* codes, size_t n) const override {
#if SIMSIMD_TARGET_NEON
        encode_neon(x, codes, n, dim, vmin, vdiff, alpha, beta);
#else
        encode_serial(x, codes, n, dim, vmin, vdiff, alpha, beta);
#endif
    }

    inline void decode(const uint8_t* code, float* x, size_t n) const override {
#if SIMSIMD_TARGET_NEON
        decode_neon(code, x, n, dim, alpha, beta);
#else
        decode_serial(code, x, n, dim, alpha, beta);
#endif
    }

    inline std::unique_ptr<DC<float, uint8_t>> get_asym_distance_computer(
        DistanceType type) const override {
        switch (type) {
        case DistanceType::INNER_PRODUCT:
            return std::make_unique<AsymmetricIP>(dim, alpha, beta);
        case DistanceType::L2_SQ:
            return std::make_unique<AsymmetricL2Sq>(dim, alpha, beta);
        case DistanceType::COSINE:
            return std::make_unique<AsymmetricCosine>(dim, alpha, beta);
        default:
            throw std::runtime_error("Unsupported distance type");
        }
    }

    inline std::unique_ptr<DC<uint8_t, uint8_t>> get_sym_distance_computer(
        DistanceType type) const override {
        switch (type) {
        case DistanceType::INNER_PRODUCT:
            return std::make_unique<SymmetricIP>(dim, alphaSqr, betaSqr);
        default:
            throw std::runtime_error("Unsupported distance type");
        }
    }

    inline void serialize(Serializer& serializer) const override {
        serializer.write(dim);
        auto size = dim * sizeof(float);
        serializer.write(reinterpret_cast<uint8_t*>(vmin), size);
        serializer.write(reinterpret_cast<uint8_t*>(vdiff), size);
        serializer.write(reinterpret_cast<uint8_t*>(alpha), size);
        serializer.write(reinterpret_cast<uint8_t*>(beta), size);
        serializer.write(reinterpret_cast<uint8_t*>(alphaSqr), size);
        serializer.write(reinterpret_cast<uint8_t*>(betaSqr), size);
    }

    static std::unique_ptr<SQ8Bit> deserialize(Deserializer& deserializer) {
        int dim;
        deserializer.deserializeValue(dim);
        auto sq = std::make_unique<SQ8Bit>(dim);
        auto size = dim * sizeof(float);
        deserializer.deserializeValue(reinterpret_cast<uint8_t*>(sq->vmin), size);
        deserializer.deserializeValue(reinterpret_cast<uint8_t*>(sq->vdiff), size);
        deserializer.deserializeValue(reinterpret_cast<uint8_t*>(sq->alpha), size);
        deserializer.deserializeValue(reinterpret_cast<uint8_t*>(sq->beta), size);
        deserializer.deserializeValue(reinterpret_cast<uint8_t*>(sq->alphaSqr), size);
        deserializer.deserializeValue(reinterpret_cast<uint8_t*>(sq->betaSqr), size);
        return sq;
    }

    std::unique_ptr<SQ8Bit> copy() const { return std::make_unique<SQ8Bit>(*this); }

    ~SQ8Bit() {
        delete[] vmin;
        delete[] vdiff;
        delete[] alpha;
        delete[] beta;
        delete[] alphaSqr;
        delete[] betaSqr;
    }

private:
    float* vmin;
    float* vdiff;

    // Precomputed values
    float* alpha;
    float* beta;
    float* alphaSqr;
    float* betaSqr;
};

} // namespace storage
} // namespace kuzu
