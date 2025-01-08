

#include <chrono>
#include <thread>
#include <vector>
#include <cstdint>
#include <iostream>

volatile size_t g_sum = 0;

template <bool prefetch>
__attribute__ ((noinline))
uint64_t sum(const uint8_t *data, size_t start, size_t len, size_t skip = 1) {
    uint64_t sum = 0;
    for (size_t i = start; i < len; i+= skip) {
        sum += data[i];
        if(prefetch) { __builtin_prefetch(&data[i + 4096], 0, 3); }
    }
    g_sum += sum;
    return sum;
}
template <bool prefetch>
double estimate_bandwidth(size_t threads_count, const uint8_t* data, size_t data_volume) {
    std::vector<std::thread> threads;
    threads.reserve(threads_count);
    size_t segment_length = data_volume / threads_count;
    size_t cache_line = 64;
    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
    if(threads_count == 1) {
        sum<prefetch>(data, 0, segment_length, cache_line);
    } else {
        for (size_t i = 0; i < threads_count; i++) {
            threads.emplace_back(sum<prefetch>, data, segment_length*i, segment_length*(i+1), cache_line);
        }
        for (std::thread &t : threads) {
            t.join();
        }
    }
    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
    std::chrono::duration<double, std::nano>  elapsed = end - start;
    return data_volume / elapsed.count();
}


int main() {
    size_t data_volume = 16*1024*1024*1024ULL; // 16 GB
    std::vector<uint8_t> data(data_volume);
    for (size_t i = 0; i < data_volume; i++) {
        data[i] = 1;
    }
    size_t N = 3;
    for(size_t threads = 1; threads <= std::thread::hardware_concurrency(); threads++) {
        double bw = estimate_bandwidth<false>(threads, data.data(), data_volume);
        // best out of N
        for(size_t i = 0; i < N; i++) {
            double cbw = estimate_bandwidth<false>(threads, data.data(), data_volume);
            if(cbw > bw) {
                bw = cbw;
            }
        }
        // number of threads + bandwidth in GB/s
        printf("%lu %.1f ", threads, bw);
        bw = estimate_bandwidth<true>(threads, data.data(), data_volume);
        // best out of N
        for(size_t i = 0; i < N; i++) {
            double cbw = estimate_bandwidth<true>(threads, data.data(), data_volume);
            if(cbw > bw) {
                bw = cbw;
            }
        }
        printf("%.1f\n", bw);
    }
}

