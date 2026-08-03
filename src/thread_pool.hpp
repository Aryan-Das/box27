#ifndef THREAD_POOL_HPP
#define THREAD_POOL_HPP

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <unordered_set>

template <typename Func>
class ThreadPool{
public:
    ThreadPool(int threads, Func func);

    void push(int clientFd);

    ThreadPool(const ThreadPool& other) = delete;
    
    ~ThreadPool();

private:
    std::mutex mutex_;
    std::vector<std::thread> workers_;
    std::queue<int> clientFdQueue_;
    std::condition_variable cv_;
    std::unordered_set<int> fdsBeingProcessed_;
};


#endif