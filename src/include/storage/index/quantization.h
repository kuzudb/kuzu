#pragma once

#include <vector>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include <simsimd/simsimd.h>

using namespace kuzu::common;

namespace kuzu {
    namespace storage {
        // Serial encoding functions
        inline uint8_t encode_serial(const float x, const float vmin, const float vdiff,
                                     float scalar_range) {
            auto val = std::min(std::max(x - vmin, 0.0f), vdiff);
            return std::min(int((val / vdiff) * scalar_range), (int) scalar_range - 1);
        }

        inline float compute_precomputed_value_serial(const uint8_t code, const float alpha, const float beta) {
            return code * alpha * beta;
        }

        inline void encode_serial_8bit(const float *data, uint8_t *codes, int dim, const float *vmin,
                                       const float *vdiff, const float *alpha, const float *beta, float beta_sqr) {
            float precompute_value = beta_sqr;
            for (size_t j = 0; j < dim; j++) {
                codes[j] = encode_serial(data[j], vmin[j], vdiff[j], 256.0f);
                precompute_value += compute_precomputed_value_serial(codes[j], alpha[j], beta[j]);
            }
            // Store precomputed values
            *reinterpret_cast<float *>(codes + dim) = precompute_value;
        }

        inline float decode_serial(const uint8_t code, const float alpha, const float beta) {
            return alpha * code + beta;
        }

        inline void compute_asym_l2sq_serial_8bit(const float *x, const uint8_t *y, double *result, size_t dim,
                                                  const float *alpha, const float *beta) {
            double res = 0;
            for (size_t i = 0; i < dim; i++) {
                auto code = decode_serial(y[i], alpha[i], beta[i]);
                auto xc = (x[i] - code);
                res += xc * xc;
            }
            *result = res;
        }

        inline void compute_sym_l2sq_serial_8bit(const uint8_t *x, const uint8_t *y, double *result, size_t dim,
                                                 const float *alphaSqr) {
            double res = 0;
            for (size_t i = 0; i < dim; i++) {
                int xy = x[i] - y[i];
                res += xy * xy * alphaSqr[i];
            }
            *result = res;
        }

        inline void compute_asym_ip_serial_8bit(const float *x, const uint8_t *y, double *result, size_t dim,
                                                  const float *alpha, const float *beta) {
            double xy = 0;
            for (size_t i = 0; i < dim; i++) {
                xy += x[i] * decode_serial(y[i], alpha[i], beta[i]);
            }
            *result = xy;
        }

        inline void compute_sym_ip_serial_8bit(const uint8_t *x, const uint8_t *y, double *result, size_t dim,
                                                const float *alphaSqr) {
            double xy = 0;
            for (size_t i = 0; i < dim; i++) {
                xy += x[i] * y[i] * alphaSqr[i];
            }
            // Add precomputed value (last 4 bytes)
            xy += *reinterpret_cast<const float *>(x + dim);
            xy += *reinterpret_cast<const float *>(y + dim);
            *result = xy;
        }

#if _SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_NEON
#pragma GCC push_options
#pragma GCC target("+simd")
#pragma clang attribute push(__attribute__((target("+simd"))), apply_to = function)

        inline uint32x4_t encode_neon_(const float *x, size_t i, const float *vmin, const float *vdiff,
                                       const float scalar_range) {
            float32x4_t scalar_range_vec = vdupq_n_f32(scalar_range);
            float32x4_t vmin_vec = vld1q_f32(vmin + i);
            float32x4_t vdiff_vec = vld1q_f32(vdiff + i);
            float32x4_t x_vec = vld1q_f32(x + i);

            // Clamp to [vmin, vmin + vdiff]
            float32x4_t val = vminq_f32(vmaxq_f32(vsubq_f32(x_vec, vmin_vec), vdupq_n_f32(0.0f)), vdiff_vec);

            // Scale to [0, 1]
            float32x4_t x_scaled = vdivq_f32(val, vdiff_vec);
            // Scale to [0, scalar_range) => min(x * scalar_range, scalar_range - 1)
            uint32x4_t ci = vminq_u32(vcvtq_u32_f32(vmulq_f32(x_scaled, scalar_range_vec)),
                                      vdupq_n_u32((uint32_t) scalar_range - 1));
            return ci;
        }

        inline uint8x8_t encode_neon(const float *x, size_t i, const float *vmin, const float *vdiff,
                                     const float scalar_range) {
            uint32x4_t ci_low = encode_neon_(x, i, vmin, vdiff, scalar_range);
            uint32x4_t ci_high = encode_neon_(x, i + 4, vmin, vdiff, scalar_range);
            return vmovn_u16(vcombine_u16(vmovn_u32(ci_low), vmovn_u32(ci_high)));
        }

        inline float32x4_t calc_precomputed_values_neon(uint8x8_t ci_vec, size_t i,
                                                        const float *alpha, const float *beta) {
            uint16x8_t ci_vec16 = vmovl_u8(ci_vec);
            float32x4_t ci_vec32_low = vcvtq_f32_u32(vmovl_u16(vget_low_u16(ci_vec16)));
            float32x4_t ci_vec32_high = vcvtq_f32_u32(vmovl_u16(vget_high_u16(ci_vec16)));

            // Calculate precomputed values i8 * alpha * beta
            float32x4_t precompute_values_low = vmulq_f32(vmulq_f32(ci_vec32_low, vld1q_f32(alpha + i)),
                                                          vld1q_f32(beta + i));
            float32x4_t precompute_values_high = vmulq_f32(vmulq_f32(ci_vec32_high, vld1q_f32(alpha + i + 4)),
                                                           vld1q_f32(beta + i + 4));

            return vaddq_f32(precompute_values_low, precompute_values_high);
        }

        inline void encode_neon_8bit(const float *data, uint8_t *codes, int dim, const float *vmin,
                                     const float *vdiff, const float *alpha, const float *beta) {
            int i = 0;
            float32x4_t precompute_values = vdupq_n_f32(0);
            for (; i + 8 <= dim; i += 8) {
                uint8x8_t ci_vec = encode_neon(data, i, vmin, vdiff, 256.0f);
                precompute_values = vaddq_f32(calc_precomputed_values_neon(ci_vec, i, alpha, beta),
                                              precompute_values);
                vst1_u8(codes + i, ci_vec);
            }
            auto precompute_value = vaddvq_f32(precompute_values);
            // Handle the remaining
            for (; i < dim; i++) {
                codes[i] = encode_serial(data[i], vmin[i], vdiff[i], 256.0f);
                precompute_value += compute_precomputed_value_serial(codes[i], alpha[i], beta[i]);
            }
            // Store precomputed values
            *reinterpret_cast<float *>(codes + dim) = precompute_value;
        }

#pragma clang attribute pop
#pragma GCC pop_options
#endif
#endif

#if _SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_HASWELL
#pragma GCC push_options
#pragma GCC target("avx2", "fma")
#pragma clang attribute push(__attribute__((target("avx2,fma"))), apply_to = function)

        inline __m128i encode_haswell_(const float *x, size_t i, const float *vmin, const float *vdiff,
                                       const float scalar_range) {
            __m256 scalar_range_vec = _mm256_set1_ps(scalar_range);
            __m256 vmin_vec = _mm256_loadu_ps(vmin + i);
            __m256 vdiff_vec = _mm256_loadu_ps(vdiff + i);
            __m256 x_vec = _mm256_loadu_ps(x + i);

            // Clamp to [vmin, vmin + vdiff]
            __m256 val = _mm256_min_ps(_mm256_max_ps(_mm256_sub_ps(x_vec, vmin_vec), _mm256_setzero_ps()), vdiff_vec);


            // Scale to [0, 1]
            __m256 x_scaled = _mm256_div_ps(val, vdiff_vec);

            // Scale to [0, scalar_range) => x * scalar_range
            __m256i ci = _mm256_cvtps_epi32(_mm256_mul_ps(x_scaled, scalar_range_vec));

            // Pack and clamp to [0, 255]
            __m128i ci_low = _mm256_extracti128_si256(ci, 0);
            __m128i ci_high = _mm256_extracti128_si256(ci, 1);
            __m128i packed16 = _mm_packus_epi32(ci_low, ci_high);

            return packed16;
        }

        inline __m128i encode_haswell(const float *x, size_t i, const float *vmin, const float *vdiff,
                                      const float scalar_range) {
            __m128i ci_low = encode_haswell_(x, i, vmin, vdiff, scalar_range);
            __m128i ci_high = encode_haswell_(x, i + 8, vmin, vdiff, scalar_range);
            return _mm_packus_epi16(ci_low, ci_high);
        }

        inline __m256 calc_precomputed_values_haswell(__m128i ci_vec, size_t i, const float *alpha, const float *beta) {
            // Load and extend ci_vec data
            __m256i ci_vec32 = _mm256_cvtepu8_epi32(ci_vec);          // Extend 8-bit to 32-bit integers
            __m256 ci_vec_f32 = _mm256_cvtepi32_ps(ci_vec32);          // Convert to float

            // Load alpha and beta
            __m256 alpha_vec = _mm256_loadu_ps(alpha + i);
            __m256 beta_vec = _mm256_loadu_ps(beta + i);

            // Calculate precomputed values
            __m256 precompute_values = _mm256_mul_ps(_mm256_mul_ps(ci_vec_f32, alpha_vec), beta_vec);

            return precompute_values;
        }

        inline void encode_haswell_8bit(const float *data, uint8_t *codes, int dim, const float *vmin,
                                        const float *vdiff, const float *alpha, const float *beta) {
            int i = 0;
            __m256 precompute_values = _mm256_setzero_ps();
            for (; i + 16 <= dim; i += 16) {
                __m128i ci_vec = encode_haswell(data, i, vmin, vdiff, 256.0f);
                precompute_values = _mm256_add_ps(calc_precomputed_values_haswell(ci_vec, i, alpha, beta),
                                                  precompute_values);
                _mm_storeu_si128((__m128i *) (codes + i), ci_vec);
            }
            __m256 temp = _mm256_hadd_ps(precompute_values, precompute_values);
            __m128 sum128 = _mm_add_ps(_mm256_castps256_ps128(temp), _mm256_extractf128_ps(temp, 1));
            sum128 = _mm_hadd_ps(sum128, sum128);
            sum128 = _mm_hadd_ps(sum128, sum128);

            auto precompute_value = _mm_cvtss_f32(sum128);
            // Handle the remaining
            for (; i < dim; i++) {
                codes[i] = encode_serial(data[i], vmin[i], vdiff[i], 256.0f);
                precompute_value += compute_precomputed_value_serial(codes[i], alpha[i], beta[i]);
            }
            // Store precomputed values
            *reinterpret_cast<float *>(codes + dim) = precompute_value;
        }

#pragma clang attribute pop
#pragma GCC pop_options
#endif // SIMSIMD_TARGET_HASWELL

#if SIMSIMD_TARGET_SKYLAKE
#pragma GCC push_options
#pragma GCC target("avx512f", "avx512vl", "bmi2")
#pragma clang attribute push(__attribute__((target("avx512f,avx512vl,bmi2"))), apply_to = function)
        inline __m512 decode_skylake(const __m128i ci_vec, size_t i, const float *alpha, const float *beta) {
            __m512i ci_vec32 = _mm512_cvtepu8_epi32(ci_vec);
            __m512 ci_vec_float = _mm512_cvtepi32_ps(ci_vec32);

            // Load alpha and beta
            __m512 alpha_vec = _mm512_loadu_ps(alpha + i);
            __m512 beta_vec = _mm512_loadu_ps(beta + i);

            // x = alpha * ci + beta
            __m512 x_vec = _mm512_fmadd_ps(alpha_vec, ci_vec_float, beta_vec);

            return x_vec;
        }

        inline void compute_asym_l2sq_skylake_8bit(const float *x, const uint8_t *y, double *result, size_t dim,
                                              const float *alpha, const float *beta) {
            __m512 d2_vec = _mm512_setzero();
            __m512 y_vec, x_vec, d_vec;
            size_t i = 0;
            for (; i + 16 <= dim; i += 16) {
                x_vec = _mm512_loadu_ps(x + i);
                __m128i codes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(y + i));
                y_vec = decode_skylake(codes, i, alpha, beta);
                d_vec = _mm512_sub_ps(x_vec, y_vec);
                d2_vec = _mm512_fmadd_ps(d_vec, d_vec, d2_vec);
            }
            *result = _mm512_reduce_add_ps(d2_vec);
        }

        inline void compute_sym_l2sq_skylake_8bit(const uint8_t *x, const uint8_t *y, double *result, size_t dim,
                                                  const float *alphaSqr) {
            // TODO
        }

        inline void compute_asym_ip_skylake_8bit(const float *x, const uint8_t *y, double *result, size_t dim,
                                                 const float *alpha, const float *beta) {
            __m512 xy_vec = _mm512_setzero();
            __m512 y_vec, x_vec, d_vec;
            size_t i = 0;
            for (; i + 16 <= dim; i += 16) {
                x_vec = _mm512_loadu_ps(x + i);
                __m128i codes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(y));
                y_vec = decode_skylake(codes, i, alpha, beta);
                xy_vec = _mm512_fmadd_ps(x_vec, y_vec, xy_vec);
            }
            *result = _mm512_reduce_add_ps(xy_vec);
        }

        inline void compute_sym_ip_skylake_8bit(const uint8_t *x, const uint8_t *y, double *result, size_t dim,
                                                const float *alphaSqr) {
            __m512 xy_vec = _mm512_setzero();
            size_t i = 0;
            for (; i + 32 <= dim; i += 32) {
                __m256i x_codes = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(x));
                __m256i y_codes = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(y));
                // Convert to 16 bit integers
                __m512i x_codes16 = _mm512_cvtepu8_epi16(x_codes);
                __m512i y_codes16 = _mm512_cvtepu8_epi16(y_codes);

                // Multiply and add
                __m512i xy = _mm512_mullo_epi16(x_codes16, y_codes16);

                // Convert to 32-bit integers
                __m512 lower_half = _mm512_cvtepi32_ps(_mm512_cvtepu16_epi32(_mm512_castsi512_si256(xy)));
                __m512 upper_half = _mm512_cvtepi32_ps(_mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(xy, 1)));

                // xy_vec = _mm512_add_ps(xy_vec, lower_half);
                __m512 alphaSqr_vec_low = _mm512_loadu_ps(alphaSqr + i);
                __m512 alphaSqr_vec_high = _mm512_loadu_ps(alphaSqr + i + 16);
                xy_vec = _mm512_fmadd_ps(lower_half, alphaSqr_vec_low, xy_vec);
                xy_vec = _mm512_fmadd_ps(upper_half, alphaSqr_vec_high, xy_vec);
            }
            *result = _mm512_reduce_add_ps(xy_vec) + reinterpret_cast<const float *>(x + dim)[0] +
                      reinterpret_cast<const float *>(y + dim)[0];
        }
#pragma clang attribute pop
#pragma GCC pop_options
#endif // SIMSIMD_TARGET_SKYLAKE
#endif // SIMSIMD_TARGET_X86

        class SQ8Bit {
            static constexpr size_t HISTOGRAM_NUM_BINS = 512;
            static constexpr size_t SCALAR_RANGE = 256;
        public:
            explicit SQ8Bit(int dim, float breakPointDataRatio = 1.0f)
                    : dim(dim), codeSize(dim + 4), breakPointDataRatio(breakPointDataRatio), numTrainedVecs(0), trainingFinished(false) {
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

                // print vmin and vdiff
//                for (size_t i = 0; i < dim; i++) {
//                    printf("vmin[%d]: %f\n", i, vmin[i]);
//                    printf("vdiff[%d]: %f\n", i, vdiff[i]);
//                }

                // initialize the histogram
                histogram = std::vector<std::vector<std::atomic_uint64_t>>(dim);
                for (size_t i = 0; i < dim; i++) {
                    histogram[i] = std::vector<std::atomic_uint64_t>(HISTOGRAM_NUM_BINS);
                }
                for (size_t i = 0; i < dim; i++) {
                    for (size_t j = 0; j < HISTOGRAM_NUM_BINS; j++) {
                        histogram[i][j] = 0;
                    }
                }
            }

            SQ8Bit(const SQ8Bit &other) : dim(other.dim), codeSize(dim + 4),
                                          breakPointDataRatio(other.breakPointDataRatio),
                                          numTrainedVecs(other.numTrainedVecs.load()), trainingFinished(other.trainingFinished) {
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

                // print vmin and vdiff
//                for (size_t i = 0; i < dim; i++) {
//                    printf("vmin[%d]: %f\n", i, vmin[i]);
//                    printf("vdiff[%d]: %f\n", i, vdiff[i]);
//                }

                histogram = std::vector<std::vector<std::atomic_uint64_t>>(dim);
                for (size_t i = 0; i < dim; i++) {
                    histogram[i] = std::vector<std::atomic_uint64_t>(HISTOGRAM_NUM_BINS);
                }
                for (size_t i = 0; i < dim; i++) {
                    for (size_t j = 0; j < HISTOGRAM_NUM_BINS; j++) {
                        histogram[i][j] = other.histogram[i][j].load();
                    }
                }
            }

            inline void batchTrain(const float *data, size_t n) {
                std::lock_guard<std::mutex> lock(mtx);
                if (trainingFinished) {
                    return;
                }

                // TODO: make this code SIMD
                for (size_t i = 0; i < n; i++) {
                    for (size_t j = 0; j < dim; j++) {
                        vmin[j] = std::min(vmin[j], data[i * dim + j]);
                        vdiff[j] = std::max(vdiff[j], data[i * dim + j]);
//                        if (data[i * dim + j] > 100) {
//                            printf("vdiff[%d]: %f\n", j, vdiff[j]);
//                            printf("data[%d]: %f\n", i * dim + j, data[i * dim + j]);
//                        }
                    }
                }
            }

            inline void determineBreakpoints(const float *data, size_t n) {
                std::lock_guard<std::mutex> lock(mtx);
                if (trainingFinished) {
                    return;
                }

                if (breakPointDataRatio >= 1.0f) {
                    return;
                }

                for (size_t i = 0; i < n; i++) {
                    for (size_t j = 0; j < dim; j++) {
                        // TODO: This should be using simd instruction.
                        // Determine the bin using vmin and vdiff.
                        auto bin = static_cast<uint64_t>(((data[i * dim + j] - vmin[j]) / vdiff[j]) * (float) HISTOGRAM_NUM_BINS);
                        bin = std::min((int) bin, (int) HISTOGRAM_NUM_BINS - 1);
                        histogram[j][bin]++;
                    }
                }
                numTrainedVecs += n;
            }

            inline void finalizeTrain() {
                std::lock_guard<std::mutex> lock(mtx);
                if (trainingFinished) {
                    return;
                }

                if (breakPointDataRatio < 1.0f) {
                    printf("Finding the smallest range\n");
                    // Now we have to find the smallest which contains at-least 70% of n of the data.
                    // Find the smallest bin range that contains at least 70% of the data
                    auto threshold = numTrainedVecs * breakPointDataRatio;
                    for (size_t i = 0; i < dim; i++) {
                        size_t start_bin = 0;
                        size_t end_bin = 0;
                        size_t min_bin_size = HISTOGRAM_NUM_BINS;
                        size_t sum = 0;
                        size_t left = 0;

                        // Sliding window approach to find the smallest range
                        for (size_t right = 0; right < HISTOGRAM_NUM_BINS; right++) {
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
                        vmin[i] = vmin[i] + (float) start_bin / (float) HISTOGRAM_NUM_BINS * vdiff[i];
                        vdiff[i] = (float) (end_bin - start_bin) / (float) HISTOGRAM_NUM_BINS * vdiff[i];
                    }
                }
//                for (size_t i = 0; i < dim; i++) {
//                    printf("vmin[%d]: %f, vdiff[%d]: %f\n", i, vmin[i], i, vdiff[i]);
//                }

                for (size_t i = 0; i < dim; i++) {
                    vdiff[i] -= vmin[i];
                    alpha[i] = vdiff[i] / SCALAR_RANGE;
                    beta[i] = (0.5f * alpha[i]) + vmin[i];
                    alphaSqr[i] = alpha[i] * alpha[i];
                    betaSqr[i] = beta[i] * beta[i];
                }

                // print
//                for (size_t i = 0; i < dim; i++) {
//                    printf("vmin[%d]: %f, vdiff[%d]: %f, alpha[%d]: %f, beta[%d]: %f, alphaSqr[%d]: %f, betaSqr[%d]: %f\n", i, vmin[i], i, vdiff[i], i, alpha[i], i, beta[i], i, alphaSqr[i], i, betaSqr[i]);
//                }

                trainingFinished = true;
            }

            inline void encode(const float *x, uint8_t *codes, size_t n) const {
                for (size_t i = 0; i < n; i++) {
                    const float *xi = x + i * dim;
                    // We need to skip the last 4 bytes as they are precomputed values
                    uint8_t *ci = codes + i * codeSize;
#if SIMSIMD_TARGET_HASWELL
                    encode_haswell_8bit(xi, ci, dim, vmin, vdiff, alpha, beta);
#elif SIMSIMD_TARGET_NEON
                    encode_neon_8bit(xi, ci, dim, vmin, vdiff, alpha, beta);
#else
                    encode_serial_8bit(xi, ci, dim, vmin, vdiff, alpha, beta, 0);
#endif
                }
            }

            inline void decode(const uint8_t *codes, float *x, size_t n) const {
                for (size_t i = 0; i < n; i++) {
                    const uint8_t *ci = codes + i * codeSize;
                    float *xi = x + i * dim;
                    for (size_t j = 0; j < dim; j++) {
                        xi[j] = decode_serial(ci[j], alpha[j], beta[j]);
                    }
                }
            }

            inline const float* getAlpha() const {
                return alpha;
            }

            inline const float* getBeta() const {
                return beta;
            }

            inline const float* getAlphaSqr() const {
                return alphaSqr;
            }

            inline void serialize(Serializer &serializer) const {
                serializer.write(dim);
                auto size = dim * sizeof(float);
                serializer.write(reinterpret_cast<uint8_t *>(vmin), size);
                serializer.write(reinterpret_cast<uint8_t *>(vdiff), size);
                serializer.write(reinterpret_cast<uint8_t *>(alpha), size);
                serializer.write(reinterpret_cast<uint8_t *>(beta), size);
                serializer.write(reinterpret_cast<uint8_t *>(alphaSqr), size);
                serializer.write(reinterpret_cast<uint8_t *>(betaSqr), size);
                serializer.write(breakPointDataRatio);
                serializer.write(numTrainedVecs.load());
                for (size_t i = 0; i < dim; i++) {
                    for (size_t j = 0; j < HISTOGRAM_NUM_BINS; j++) {
                        serializer.write(histogram[i][j].load());
                    }
                }
            }

            static std::unique_ptr<SQ8Bit> deserialize(Deserializer &deserializer) {
                int dim;
                deserializer.deserializeValue(dim);
                auto sq = std::make_unique<SQ8Bit>(dim);
                auto size = dim * sizeof(float);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->vmin), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->vdiff), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->alpha), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->beta), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->alphaSqr), size);
                deserializer.deserializeValue(reinterpret_cast<uint8_t *>(sq->betaSqr), size);
                deserializer.deserializeValue(sq->breakPointDataRatio);
                uint64_t numTrainedVecs;
                deserializer.deserializeValue(numTrainedVecs);
                sq->numTrainedVecs = numTrainedVecs;
                for (size_t i = 0; i < dim; i++) {
                    for (size_t j = 0; j < HISTOGRAM_NUM_BINS; j++) {
                        uint64_t val;
                        deserializer.deserializeValue(val);
                        sq->histogram[i][j] = val;
                    }
                }
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

        public:
            int dim; // dimension of the input vectors
            size_t codeSize; // bytes per indexed vector

        private:
            std::mutex mtx;
            bool trainingFinished = false;

            float *vmin;
            float *vdiff;

            // Precomputed values
            float *alpha;
            float *beta;
            float *alphaSqr;
            float *betaSqr;

            // Training
            float breakPointDataRatio;
            std::atomic_uint64_t numTrainedVecs;
            std::vector<std::vector<std::atomic_uint64_t>> histogram;
        };
    } // namespace storage
} // namespace kuzu
