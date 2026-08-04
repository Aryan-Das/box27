#ifndef LRU_CACHE_HPP
#define LRU_CACHE_HPP

#include <unordered_map>
#include <list>
#include <string>
#include <mutex>
#include <iterator> 
#include <optional>

class LRUCache{
public:
    LRUCache(size_t capacity)
    : capacity_ {capacity}
    {}

    std::optional<std::string> get(std::string fileName){
        std::lock_guard<std::mutex> lock(mutex_);
        if(hashmap_.contains(fileName)){
            auto it = hashmap_[fileName];
            list_.splice(list_.begin(), list_, it);
            return it->data;
        }
        return std::nullopt; 
    }

    void put(std::string fileName, std::string data){
        std::lock_guard<std::mutex> lock(mutex_);
        if(hashmap_.contains(fileName)){
            list_.erase(hashmap_[fileName]);
        }
        list_.push_front(Node {fileName, data});
        hashmap_[fileName] = list_.begin();
        if(list_.size() > capacity_){
            hashmap_.erase(std::prev(list_.end()) -> fileName);
            list_.pop_back();
        }
    }

private:
    struct Node{
        std::string fileName;
        std::string data;
    };
    std::unordered_map<std::string, std::list<Node>::iterator> hashmap_;
    std::list<Node> list_;
    std::mutex mutex_;
    const size_t capacity_;
   
};


#endif