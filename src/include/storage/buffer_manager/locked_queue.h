#pragma once

#include <mutex>
#include <queue>

template<typename T>
class LockedQueue {
    std::mutex mtx;
    std::queue<T> q;

public:
    void enqueue(T x) {
        std::scoped_lock lck{mtx};
        q.push(std::move(x));
    }

    bool try_dequeue(T& x) {
        std::scoped_lock lck{mtx};
        if (!q.empty()) {
            x = q.front();
            q.pop();
            return true;
        }
        return false;
    }

    size_t size() {
        std::scoped_lock lck{mtx};
        return q.size();
    }
};
