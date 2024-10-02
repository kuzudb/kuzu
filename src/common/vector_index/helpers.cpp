#include "common/vector_index/helpers.h"
#include "math.h"

namespace kuzu {
    namespace common {
        constexpr int DUMMY_ITER = 5;
        template<typename T>
        BinaryHeap<T>::BinaryHeap(int capacity)
                : capacity{capacity}, actual_size{0} {
            // Initialize nodes with invalid entries
            nodes.resize(capacity + 1, T());  // +1 for 1-based indexing
            minElement.store(&nodes[1], std::memory_order_relaxed);
        }

        template<typename T>
        void BinaryHeap<T>::push(T node) {
            if (actual_size == capacity) {
                if (node < nodes[1]) {
                    return;  // Node is not closer/farther enough to be inserted into the heap
                }
                popMinFromHeap();  // Remove the root (minimum or maximum, depending on heap type)
                actual_size--;
            }
            actual_size++;
            pushToHeap(node);
            minElement.store(&nodes[1], std::memory_order_relaxed);
        }

        template<typename T>
        inline void BinaryHeap<T>::pushToHeap(T node) {
            size_t i = actual_size, i_father;
            // Heap up to maintain the heap property
            while (i > 1) {
                i_father = i >> 1;  // Get parent index
                if (node >= nodes[i_father]) {
                    break;  // Heap property is satisfied
                }
                nodes[i] = nodes[i_father];  // Move parent down
                i = i_father;
            }
            nodes[i] = node;  // Place new node in the correct position
        }

        template<typename T>
        T BinaryHeap<T>::popMin() {
            if (actual_size == 0) {
                return T();
            }
            T minNode = nodes[1];
            popMinFromHeap();
            actual_size--;
            if (actual_size == 0) {
                nodes[1] = T();
            }
            minElement.store(&nodes[1], std::memory_order_relaxed);
            return minNode;
        }

        template<typename T>
        void BinaryHeap<T>::popMinFromHeap() {
            T node = nodes[actual_size];  // Replace the root with the last node
            size_t i = 1, i1, i2;
            // Heap down to maintain the heap property
            while (true) {
                i1 = i << 1;  // Left child index
                i2 = i1 + 1;  // Right child index
                if (i1 > actual_size) {
                    break;  // No children
                }
                if (i2 > actual_size || nodes[i1] < nodes[i2]) {
                    // If only one child exists or the left child is smaller/larger (depending on heap type)
                    if (node <= nodes[i1]) {
                        break;  // Heap property is satisfied
                    }
                    nodes[i] = nodes[i1];  // Move child up
                    i = i1;
                } else {
                    // Right child is smaller/larger
                    if (node <= nodes[i2]) {
                        break;  // Heap property is satisfied
                    }
                    nodes[i] = nodes[i2];  // Move child up
                    i = i2;
                }
            }
            nodes[i] = node;  // Place the last node in the correct position
        }

        template<typename T>
        void BinaryHeap<T>::popMaxFromHeap() {
            T node = nodes[actual_size];  // Replace the root with the last node
            size_t i = 1, i1, i2;
            // Heap down to maintain the heap property
            while (true) {
                i1 = i << 1;  // Left child index
                i2 = i1 + 1;  // Right child index
                if (i1 > actual_size) {
                    break;  // No children
                }
                if (i2 > actual_size || nodes[i1] > nodes[i2]) {
                    // If only one child exists or the left child is smaller/larger (depending on heap type)
                    if (node >= nodes[i1]) {
                        break;  // Heap property is satisfied
                    }
                    nodes[i] = nodes[i1];  // Move child up
                    i = i1;
                } else {
                    // Right child is smaller/larger
                    if (node >= nodes[i2]) {
                        break;  // Heap property is satisfied
                    }
                    nodes[i] = nodes[i2];  // Move child up
                    i = i2;
                }
            }
            nodes[i] = node;  // Place the last node in the correct position
        }

        inline uint64_t randomFnv1a(uint64_t &seed) {
            const static uint64_t offset = 14695981039346656037ULL;
            const static uint64_t prime = 1099511628211;

            uint64_t hash = offset;
            hash ^= seed;
            hash *= prime;
            seed = hash;
            return hash;
        }

        template<typename T>
        ParallelMultiQueue<T>::ParallelMultiQueue(int num_queues, int reserve_size) : maxSize(std::ceil(reserve_size / num_queues)) {
            queues.reserve(num_queues);
            for (int i = 0; i < num_queues; i++) {
                queues.emplace_back(std::make_unique<BinaryHeap<T>>(maxSize));
            }

            queueSizes = std::vector<std::atomic_int>(num_queues);
            // store 0
            for (int i = 0; i < num_queues; i++) {
                queueSizes[i].store(0);
            }
        }

        template<typename T>
        int ParallelMultiQueue<T>::getRandQueueIndex() const {
            static std::atomic<int> num_threads_registered{0};
            thread_local uint64_t seed = 2758756369U + num_threads_registered++;
            return randomFnv1a(seed) % queues.size();
        }

        template<typename T>
        void ParallelMultiQueue<T>::push(T val) {
            int q_id = getRandQueueIndex();
            queues[q_id]->lock();
            queues[q_id]->push(val);
            queueSizes[q_id].fetch_add(1);
            queues[q_id]->unlock();
        }

        template<typename T>
        void ParallelMultiQueue<T>::bulkPush(T *vals, int num_vals) {
            std::vector<std::vector<int>> vals_per_queue(queues.size());
            for (int i = 0; i < num_vals; i++) {
                int q_id = getRandQueueIndex();
                vals_per_queue[q_id].push_back(i);
            }
            std::unordered_set<int> pendingQueues(queues.size());
            for (int i = 0; i < queues.size(); i++) {
                pendingQueues.insert(i);
            }

            while (!pendingQueues.empty()) {
                for (const int q_id : pendingQueues) {
                    if (vals_per_queue[q_id].empty()) {
                        pendingQueues.erase(q_id);
                        break;
                    }
                    if (queues[q_id]->try_lock()) {
                        auto size = vals_per_queue[q_id].size();
                        for (int j = 0; j < size; j++) {
                            queues[q_id]->push(vals[vals_per_queue[q_id][j]]);
                        }
                        queueSizes[q_id].fetch_add(size);
                        queues[q_id]->unlock();
                        vals_per_queue[q_id].clear();
                        pendingQueues.erase(q_id);
                        break;
                    }
                }
            }
        }

        template<typename T>
        T ParallelMultiQueue<T>::popMin() {
            bool updateByOtherThreads = false;
            do {
                updateByOtherThreads = false;
                for (auto dummy_i = 0; dummy_i < DUMMY_ITER; dummy_i++) {
                    int i, j;
                    i = getRandQueueIndex();
                    do {
                        j = getRandQueueIndex();
                    } while (i == j);

                    auto topI = queues[i]->getMinElement();
                    auto topJ = queues[j]->getMinElement();

                    if (topI->isInvalid() && topJ->isInvalid()) {
                        continue;
                    }

                    const T *min;
                    int minIdx;
                    if (topI->isInvalid()) {
                        min = topJ;
                        minIdx = j;
                    } else if (topJ->isInvalid()) {
                        min = topI;
                        minIdx = i;
                    } else {
                        min = *topI < *topJ ? topI : topJ;
                        minIdx = *topI < *topJ ? i : j;
                    }

                    queues[minIdx]->lock();
                    if (min == queues[minIdx]->top()) {
                        auto res = queues[minIdx]->popMin();
                        queueSizes[minIdx].fetch_sub(1);
                        queues[minIdx]->unlock();
                        return res;
                    }
                    updateByOtherThreads = true;
                    queues[minIdx]->unlock();
                }
            } while (updateByOtherThreads);
            return T();
        }

        template<typename T>
        const T *ParallelMultiQueue<T>::top() {
            bool updateByOtherThreads = false;
            do {
                updateByOtherThreads = false;
                for (std::size_t dummy_i = 0; dummy_i < DUMMY_ITER; dummy_i++) {
                    int i, j;
                    i = getRandQueueIndex();
                    do {
                        j = getRandQueueIndex();
                    } while (i == j);

                    auto topI = queues[i]->getMinElement();
                    auto topJ = queues[j]->getMinElement();

                    if (topI->isInvalid() && topJ->isInvalid()) {
                        continue;
                    }

                    const T *min;
                    int minIdx;
                    if (topI->isInvalid()) {
                        min = topJ;
                        minIdx = j;
                    } else if (topJ->isInvalid()) {
                        min = topI;
                        minIdx = i;
                    } else {
                        min = *topI < *topJ ? topI : topJ;
                        minIdx = *topI < *topJ ? i : j;
                    }

                    queues[minIdx]->lock();
                    if (min == queues[minIdx]->top()) {
                        queues[minIdx]->unlock();
                        return min;
                    }
                    updateByOtherThreads = true;
                    queues[minIdx]->unlock();
                }
            } while (updateByOtherThreads);

            return nullptr;
        }

        template<typename T>
        int ParallelMultiQueue<T>::size() {
            int totalSize = 0;
            for (int i = 0; i < queues.size(); i++) {
                totalSize += std::min(queueSizes[i].load(), maxSize);
            }
            return totalSize;
        }

        template
        class BinaryHeap<NodeDistCloser>;

        template
        class BinaryHeap<NodeDistFarther>;

        template
        class ParallelMultiQueue<NodeDistCloser>;

        template
        class ParallelMultiQueue<NodeDistFarther>;
    } // namespace common
} // namespace kuzu
