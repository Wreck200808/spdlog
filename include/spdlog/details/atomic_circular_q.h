#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <cassert>
#include <spdlog/common.h>

namespace spdlog {
namespace details {

template <typename T>
class atomic_circular_q {
    const size_t size_;
    const size_t mask_;
    std::unique_ptr<T[]> buffer_;
    alignas(128) std::atomic<size_t> head_;
    alignas(128) std::atomic<size_t> tail_;
    size_t overrun_counter_ = 0;

public:
    explicit atomic_circular_q(size_t size)
        : size_(size),
          mask_(size - 1),
          buffer_(new T[size]),
          head_(0),
          tail_(0) {
        assert((size & (size - 1)) == 0);
    }
    
    bool push_back(T &&item) {
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t next_tail = (tail + 1) & mask_;
        if (next_tail == head_.load(std::memory_order_acquire)) {
            return false;  // queue is full
        }
        buffer_[tail] = std::move(item);
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    // push back, overrun (oldest) item if no room left
    void push_back_nowait(T &&item) {
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t next_tail = (tail + 1) & mask_;
        if (next_tail == head_.load(std::memory_order_acquire)) {
            head_.store((head_.load(std::memory_order_relaxed) + 1) & mask_, std::memory_order_release);
            ++overrun_counter_;
        }
        buffer_[tail] = std::move(item);
        tail_.store(next_tail, std::memory_order_release);
    }
    
    bool pop_front(T &item) {
        size_t head = head_.load(std::memory_order_acquire);
        if (head == tail_.load(std::memory_order_acquire)) {
            return false;  // queue is empty
        }
        item = std::move(buffer_[head]);
        head_.store((head + 1) & mask_, std::memory_order_release);
        return true;
    }

    size_t size() const {
        return (tail_.load(std::memory_order_acquire) - head_.load(std::memory_order_acquire)) & mask_;
    }

    size_t overrun_counter() const { return overrun_counter_; }

    void reset_overrun_counter() { overrun_counter_ = 0; }

};

}  // namespace details
}  // namespace spdlog