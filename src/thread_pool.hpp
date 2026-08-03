#ifndef THREAD_POOL_HPP
#define THREAD_POOL_HPP

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <unordered_set>
#include <functional>

class ThreadPool{
public:
    ThreadPool(int threads) 
    : stop_ { false }
    
    {
        for(int i{0}; i < threads; ++i){
        workers_.emplace_back([this]{
            
            while(true){
                std::function<void()> task;
                std::unique_lock<std::mutex> lock(queueMutex_);
                cv_.wait(lock, [&]{return stop_ ||  !tasks_.empty(); });
                if(stop_ && tasks_.empty()) return;
                task = std::move(tasks_.front());
                tasks_.pop();
                lock.unlock();
                task();

            }
        });
    }
    }


    void push(std::function<void()> task){
        {
            std::lock_guard lock(queueMutex_);
            tasks_.push(std::move(task));
        }

        cv_.notify_one();
    }

    ThreadPool(const ThreadPool& other) = delete;
    
    ~ThreadPool(){
        cv_.notify_all();
        for(auto& worker : workers_){
            worker.join();
        }
    }

private:
    std::mutex queueMutex_;
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::condition_variable cv_;

    bool stop_;
};


#endif

