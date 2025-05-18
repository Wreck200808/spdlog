#pragma once

#include <spdlog/details/atomic_circular_q.h>

#include <unordered_map>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace spdlog {
namespace details {
template <typename T>
class queue_manager {
public:
    using item_type = T;

    explicit queue_manager(size_t max_items)
        : max_items_(max_items) {
        assert((max_items & (max_items - 1)) == 0);  // Ensure size is a power of 2
        auto local_queue = get_local_queue();
    }

    ~queue_manager() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& i : allQueues) {
            delete i.second;
        }
        allQueues.clear();
    }

    spdlog::details::atomic_circular_q<T>* get_local_queue() {
        std::thread::id tid = std::this_thread::get_id();
        std::lock_guard<std::mutex> lock(queue_mutex_);
        auto it = allQueues.find(tid);
        if (it == allQueues.end()) {
            auto* buf = new spdlog::details::atomic_circular_q<T>(max_items_);
            allQueues[tid] = buf;
            return buf;
        }
        return it->second;
    }

    // try to enqueue and block if no room left
    void enqueue(T&& item) {
        auto local_queue = get_local_queue();
        while (local_queue->push_back(std::move(item)) == false) {
            std::this_thread::yield();
        }
        push_cv_.notify_one();
    }

    // enqueue immediately. overrun oldest message in the queue if no room left.
    void enqueue_nowait(T&& item) {
        auto local_queue = get_local_queue();
        local_queue->push_back_nowait(std::move(item));
        push_cv_.notify_one();
    }

    void enqueue_if_have_room(T&& item) {
        auto local_queue = get_local_queue();
        if (!local_queue->push_back(std::move(item))) {
            ++discard_counter_;
        } else {
            push_cv_.notify_one();
        }
    }

    bool dequeue_for(T& popped_item, std::chrono::milliseconds wait_duration) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (!push_cv_.wait_for(lock, wait_duration, [this, &popped_item] { 
            for (auto& q : allQueues) {
                if (q.second->pop_front(popped_item)) {
                    return true;
                }
            }
            return false;
        })) {
            return false;  // timeout
        }
    }

    // blocking dequeue without a timeout.
    void dequeue(T& popped_item) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        push_cv_.wait(lock, [this, &popped_item] { 
            for (auto& q : allQueues) {
                if (!q.second->pop_front(popped_item)) {
                    return true;
                }
            }
            return false;
        });
    }

    size_t overrun_counter() {
        int overrun_counter = 0;
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& q : allQueues) {
            overrun_counter += q.second->overrun_counter();
        }
        return overrun_counter;
    }

    size_t discard_counter() { return discard_counter_.load(std::memory_order_relaxed); }

    size_t size() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        size_t total_size = 0;
        for (auto& q : allQueues) {
            total_size += q.second->size();
        }
        return total_size;
    }

    void reset_overrun_counter() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& q : allQueues) {
            q.second->reset_overrun_counter();
        }
    }

    void reset_discard_counter() { discard_counter_.store(0, std::memory_order_relaxed); }

private:
    int max_items_;
    std::unordered_map<std::thread::id, spdlog::details::atomic_circular_q<T>*> allQueues;

    std::mutex queue_mutex_;
    std::condition_variable push_cv_;
    std::atomic<size_t> discard_counter_{0};
};
}  // namespace details
}  // namespace spdlog
