#pragma once

#include <sys/stat.h>

#include <cstring>
#include <fstream>
#include <iostream>

#include "common/types/types.h"
#include <sys/fcntl.h>
#include "atomic"
#include "thread"

namespace kuzu {
namespace common {

#define IS_ALIGNED(X, Y) ((uint64_t)(X) % (uint64_t)(Y) == 0)

// TODO(gaurav): Improve this class to be more efficient.
class VisitedTable {
public:
    explicit VisitedTable(size_t size) : visited(size, 0), visited_id(1){};
    inline void set(vector_id_t id) { visited[id] = visited_id; }
    inline bool get(vector_id_t id) { return visited[id] == visited_id; }
    inline void reset() {
        visited_id++;
        //        printf("Resetting visited table\n");
        if (visited_id == 250) {
            std::fill(visited.begin(), visited.end(), 0);
            visited_id = 1;
        }
    }
    inline uint8_t* data() { return visited.data(); }

private:
    std::vector<uint8_t> visited;
    uint8_t visited_id;
};

        struct NodeDistCloser {
            explicit NodeDistCloser(vector_id_t id = INVALID_VECTOR_ID, double dist = std::numeric_limits<double>::max())
                    : id(id), dist(dist) {}

            vector_id_t id;
            double dist;

            bool operator<(const NodeDistCloser &other) const {
                return dist < other.dist;
            }

            bool operator>(const NodeDistCloser &other) const {
                return dist > other.dist;
            }

            bool operator>= (const NodeDistCloser &other) const {
                return dist >= other.dist;
            }

            bool operator<= (const NodeDistCloser &other) const {
                return dist <= other.dist;
            }

            bool operator==(const NodeDistCloser &other) const {
                return id == other.id && dist == other.dist;
            }

            inline bool isInvalid() const {
                return id == INVALID_VECTOR_ID;
            }
        };

        struct NodeDistFarther {
            explicit NodeDistFarther(vector_id_t id = INVALID_VECTOR_ID, double dist = -1)
                    : id(id), dist(dist) {}

            vector_id_t id;
            double dist;

            bool operator<(const NodeDistFarther &other) const {
                return dist > other.dist;
            }

            bool operator>(const NodeDistFarther &other) const {
                return dist < other.dist;
            }

            bool operator>= (const NodeDistFarther &other) const {
                return dist <= other.dist;
            }

            bool operator<= (const NodeDistFarther &other) const {
                return dist >= other.dist;
            }

            bool operator==(const NodeDistFarther &other) const {
                return id == other.id && dist == other.dist;
            }

            inline bool isInvalid() const {
                return id == INVALID_VECTOR_ID;
            }
        };

static void allocAligned(void** ptr, size_t size, size_t align) {
    *ptr = nullptr;
    if (!IS_ALIGNED(size, align)) {
        throw InternalException(stringFormat("size: %lu, align: %lu\n", size, align));
    }
#ifdef __APPLE__
    int err = posix_memalign(ptr, align, size);
    if (err) {
        throw InternalException(stringFormat("posix_memalign failed with error code %d\n", err));
    }
#else
    *ptr = ::aligned_alloc(align, size);
#endif
    if (*ptr == nullptr) {
        throw InternalException("aligned_alloc failed\n");
    }
}

static float* readFvecFile(const char* fName, size_t* d_out, size_t* n_out) {
    FILE* f = fopen(fName, "r");
    if (!f) {
        fprintf(stderr, "could not open %s\n", fName);
        perror("");
        abort();
    }
    int d;
    fread(&d, 1, sizeof(int), f);
    KU_ASSERT_MSG((d > 0 && d < 1000000), "unreasonable dimension");
    fseek(f, 0, SEEK_SET);
    struct stat st {};
    fstat(fileno(f), &st);
    size_t sz = st.st_size;
    KU_ASSERT_MSG(sz % ((d + 1) * 4) == 0, "weird file size");
    size_t n = sz / ((d + 1) * 4);
    *d_out = d;
    *n_out = n;
    auto* x = new float[n * (d + 1)];
    printf("x: %p\n", x);
    size_t nr = fread(x, sizeof(float), n * (d + 1), f);
    KU_ASSERT_MSG(nr == n * (d + 1), "could not read whole file");

    // TODO: Round up the dimensions to the nearest multiple of 8, otherwise the below code will not
    // work
    float* align_x;
    allocAligned(((void**)&align_x), n * d * sizeof(float), 8 * sizeof(float));
    printf("align_x: %p\n", align_x);

    // copy data to aligned memory
    for (size_t i = 0; i < n; i++) {
        memcpy(align_x + i * d, x + 1 + i * (d + 1), d * sizeof(float));
    }

    // free original memory
    delete[] x;
    fclose(f);
    return align_x;
}

static int* readIvecFile(const char* fName, size_t* d_out, size_t* n_out) {
    return (int*)readFvecFile(fName, d_out, n_out);
}

static void loadFromFile(const std::string& path, uint8_t* data, size_t size) {
    std::ifstream inputFile(path, std::ios::binary);
    inputFile.read(reinterpret_cast<char*>(data), size);
    inputFile.close();
}

// TODO: Use bitset to store the visited table
class AtomicVisitedTable {
public:
    explicit AtomicVisitedTable(size_t size) : visited(size), visited_id(1) {
        for (auto &v: visited) {
            v.store(0, std::memory_order_relaxed);
        }
    };

    inline void set(vector_id_t id) {
        visited[id].store(visited_id, std::memory_order_relaxed);
    }

    inline bool get(vector_id_t id) {
        return visited[id].load(std::memory_order_relaxed) == visited_id;
    }

    bool getAndSet(vector_id_t id) {
        if (visited[id].load(std::memory_order_relaxed) == visited_id) {
            return false;
        }
        // Reduce the number of CAS operations
        uint8_t expected = 0;
        return visited[id].compare_exchange_weak(expected, visited_id);
    }

    inline void reset() {
        // TODO: Not ideal for perf
        for (auto &v: visited) {
            v.store(0, std::memory_order_relaxed);
        }
    }

private:
    std::vector<std::atomic<uint8_t>> visited;
    const uint8_t visited_id;
};

// TODO: Maybe optimize this class if needed
struct Spinlock {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;

    // Lock the spinlock
    void lock() {
        while (flag.test_and_set(std::memory_order_acquire)) {
            // Yield the processor to reduce contention (useful in high-contention scenarios)
            std::this_thread::yield();
        }
    }

    // Unlock the spinlock
    void unlock() {
        flag.clear(std::memory_order_release);
    }

    // Try to lock the spinlock, returns true if the lock is acquired, false otherwise
    bool try_lock() {
        // Attempt to acquire the lock without blocking
        return !flag.test_and_set(std::memory_order_acquire);
    }
};

// Optimized version of binary heap for multi-threaded environments
template<typename T>
class BinaryHeap {
public:
    explicit BinaryHeap(int capacity);

    void push(T val);

    T popMin();

    inline const int size() const {
        return actual_size;
    };

    inline const T *top() const {
        return &nodes[1];
    }

    inline void lock() {
        mtx.lock();
    }

    inline bool try_lock() {
        return mtx.try_lock();
    }

    inline void unlock() {
        mtx.unlock();
    }

    inline const T *getMinElement() {
        return minElement.load(std::memory_order_relaxed);
    }

private:
    void pushToHeap(T val);

    void popMinFromHeap();

    void popMaxFromHeap();

private:
    int capacity;
    int actual_size;
    std::vector<T> nodes;
    Spinlock mtx; // Spinlock

    // Redundantly store the min element separately
    std::atomic<T *> minElement;
};

// Based on this paper: https://arxiv.org/abs/1411.1209
template<typename T>
class ParallelMultiQueue {
public:
    explicit ParallelMultiQueue(int num_queues, int reserve_size);

    void push(T val);

    void bulkPush(T *vals, int num_vals);

    T popMin();

    const T *top();

    int size();

private:
    int getRandQueueIndex() const;

public:
    std::vector<std::unique_ptr<BinaryHeap<T>>> queues;
    std::vector<std::atomic_int> queueSizes;
    const int maxSize;
};

// Bit vector based visited table
class BitVectorVisitedTable {
private:
    uint8_t *data_;
    uint32_t num_bytes_;
public:
    explicit BitVectorVisitedTable(uint32_t num_bits)
    {
        num_bytes_ = (num_bits + 7) >> 3; // (n + 7) / 8
        data_ = new uint8_t[num_bytes_];
        memset(data_, 0, num_bytes_);
    }

    ~BitVectorVisitedTable()
    {
        delete[] data_;
        num_bytes_ = 0;
    }

    uint8_t atomic_is_bit_set(const uint32_t x)
    {
        const uint32_t i_byte = x >> 3;
        const uint32_t i_bit = x - (i_byte << 3);
        return (__atomic_load_n(data_ + i_byte, __ATOMIC_ACQUIRE) >> i_bit) & 1;
    }

    void atomic_reset_bit(const uint32_t x)
    {
        const uint32_t i_byte = x >> 3;
        const uint32_t i_bit = x - (i_byte << 3);
        __atomic_and_fetch(data_ + i_byte, ~(1 << i_bit), __ATOMIC_RELEASE);
    }

    void atomic_set_bit(const uint32_t x)
    {
        const uint32_t i_byte = x >> 3;
        const uint32_t i_bit = x - (i_byte << 3);
        __atomic_or_fetch(data_ + i_byte, 1 << i_bit, __ATOMIC_RELEASE);
    }

    uint8_t is_bit_set(const uint32_t x)
    {
        const uint32_t i_byte = x >> 3;
        const uint32_t i_bit = x - (i_byte << 3);
        return (data_[i_byte] >> i_bit) & 1;
    }

    void reset_bit(const uint32_t x)
    {
        const uint32_t i_byte = x >> 3;
        const uint32_t i_bit = x - (i_byte << 3);
        data_[i_byte] &= ~(1 << i_bit);
    }

    void resize(const uint32_t num_bits)
    {
        delete[] data_;
        num_bytes_ = (num_bits + 7) >> 3; // (n + 7) / 8
        data_ = new uint8_t[num_bytes_];
        memset(data_, 0, num_bytes_);
    }

    void set_bit(const uint32_t x)
    {
        const uint32_t i_byte = x >> 3;
        const uint32_t i_bit = x - (i_byte << 3);
        data_[i_byte] |= (1 << i_bit);
    }

    void reset()
    {
        memset(data_, 0, num_bytes_);
    }
};
} // namespace common
} // namespace kuzu
