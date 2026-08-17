#include <iostream>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <csignal>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <stdexcept>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <mutex>
#include <optional>


#include "utils.hpp"
#include "mime.hpp"
#include "thread_pool.hpp"
#include "lru_cache.hpp"
#include "sha256.hpp"

constexpr int MAX = 1000;




class Server{
public:
    Server(uint32_t port, size_t threads, size_t cacheCapacity)
    : fd_ { socket(AF_INET, SOCK_STREAM, 0) }
    , epollFd_ {epoll_create1(0)}
    , serverAddress_ { }
    , ev { }
    , running_ { true }
    , threadPool_ { threads }
    , cache_ {cacheCapacity}
    {
        serverAddress_.sin_family = AF_INET;
        serverAddress_.sin_port = htons(port);
        serverAddress_.sin_addr.s_addr = htonl(INADDR_ANY);

        if(bind(fd_, (struct sockaddr*)&serverAddress_, sizeof(serverAddress_)) < 0){
            int err_code = errno; 
            std::cerr << "bind failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
            throw std::runtime_error("Failed to bind socket");
        }
        if(listen(fd_, 512) < 0) {
            int err_code = errno; 
            std::cerr << "listen failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
            throw std::runtime_error("Failed to listen to socket");
        }
        ev.events = EPOLLIN;
        ev.data.fd = fd_;
        int serverEv = epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd_, &ev);
        if(serverEv < 0){
            throw std::runtime_error("Failed to add server epoll_event");
        }
    }
    void main(){
        signal(SIGPIPE, SIG_IGN);
        while(true){
            int waitResult = epoll_wait(epollFd_, evList, MAX, -1);
            if(waitResult < 0) {
                int err_code = errno; 
                std::cerr << "epoll_wait failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
            }
            else{
                for(int i {0}; i < waitResult; ++i){
                    if (evList[i].data.fd == fd_){
                        std::cout << "accepting connection"<< std::endl;
                        int clientFd = accept(fd_, nullptr, nullptr);
                        if(clientFd < 0){
                            int err_code = errno; 
                            std::cerr << "accept failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                        }else{
                            std::cout << "accepted"<< std::endl;
                            int flags = fcntl(clientFd, F_GETFL, 0);
                            if (flags == -1) {
                                int err_code = errno; 
                                std::cerr << "fcntl failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                            }
                            fcntl(clientFd, F_SETFL, flags | O_NONBLOCK);
                            struct epoll_event clientEv;
                            clientEv.events = EPOLLIN;
                            clientEv.data.fd = clientFd;
                            if(epoll_ctl(epollFd_, EPOLL_CTL_ADD, clientFd, &clientEv) < 0){
                                int err_code = errno; 
                                std::cerr << "epoll_ctl failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";    
                            }
                        }
                    }
                    else{
                        std::lock_guard lock(fdsMutex_);
                        int clientFd = evList[i].data.fd;
                        if(!fdsBeingProcessed_.contains(clientFd)) {
                            fdsBeingProcessed_.insert(clientFd);
                            threadPool_.push([this, clientFd](){
                                handleClient(clientFd);
                            });
                        }
                        
                    }
                }
            }
        }
    }
    
    
    bool handleClient(int clientFd){
        FdGuard fdGuard {fdsBeingProcessed_, fdsMutex_, clientFd};
   
        ConnectionState connState;
        {
        std::lock_guard<std::mutex> guard(connectionStatesMutex_);
        if(!connectionStates_.contains(clientFd)){
            connectionStates_[clientFd] = ConnectionState{};
        }
        connState = connectionStates_[clientFd];
        }
        if(connState.state != Complete){
            char buffer[1024] = { 0 };
            int bytes_received = recv(clientFd, buffer, sizeof(buffer), 0);
            if(bytes_received < 0){
                int err_code = errno; 
                if(err_code != 11)
                    std::cerr << "recv failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                
                return false;
            }
            else if(bytes_received == 0){
                epoll_ctl(epollFd_, EPOLL_CTL_DEL, clientFd, nullptr);
                close(clientFd);
                {
                std::lock_guard<std::mutex> guard(connectionStatesMutex_);
                connectionStates_.erase(clientFd);
                }
                
                return false;
            }
            connState.buffer.append(buffer, bytes_received);
            while(connState.state != Complete){
                if(connState.state == Headers){
                    if (connState.buffer.find("\r\n\r\n") == std::string::npos)
                        break;
                    ParseResult parsed = parseHttp(&connState.buffer[0], connState.buffer.size());
                    std::stringstream httpResponse; 
                    if(parsed.result < 0){
                        std::cerr << "HTTP parse failed with error code: " << parsed.result << "\n";
                        const char* errorMessage = "400 Error: Bad Request";
                        httpResponse << "HTTP/1.1 400 BAD REQUEST\r\nContent-Length: "
                                    << strlen(errorMessage) << "\r\n"
                                    << "Content-Type: " << "text/plain" 
                                    << "\r\n\r\n" 
                                    << errorMessage;
                        int bytes_sent = write(clientFd, httpResponse.str().c_str(), httpResponse.str().size());
                        if(bytes_sent < 0){
                            int err_code = errno; 
                            if (err_code != EAGAIN){
                                std::cerr << "write failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                                close(clientFd);
                                {
                                std::lock_guard<std::mutex> guard(connectionStatesMutex_);
                                connectionStates_.erase(clientFd);
                                }
                                return false;
                            }
                            
                        }
                        {
                        std::lock_guard<std::mutex> guard(connectionStatesMutex_);
                        connectionStates_[clientFd] = connState;
                        }
                        return false;
                    }
                    connState.http = parsed.req;
                    connState.state = Content;

                }
                else if(connState.state == Content){
                    size_t fullLength = connState.buffer.find("\r\n\r\n") + 4
                                     + connState.http->contentLength;
                    if (connState.buffer.size() >= fullLength) {
                        
                        connState.state = Complete;
                    } else {
                        break; 
                    }
                }
            }
            {
            std::lock_guard<std::mutex> guard(connectionStatesMutex_);
            connectionStates_[clientFd] = connState;
            }
            if (connState.state != Complete)
                return false;

            
            
        }
  
        bool sentfile = false;
        
        
        if (connState.http != std::nullopt){
            std::stringstream httpResponse; 
            
            HTTPRequest req = connState.http.value();
            if(req.method == "GET"){
                
            
                std::string fileContents; 
                auto cacheResult = cache_.get(req.path.substr(1));
                
                if(cacheResult == std::nullopt){
                    int fd = open(req.path.substr(1).c_str(), O_RDONLY);
                    if(fd < 0){
                        int err_code = errno; 
                        std::cerr << "open failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                        const char* errorMessage = "404 Error: File Not Found";
                        httpResponse << "HTTP/1.1 404 NOT FOUND\r\nContent-Length: " 
                                    << strlen(errorMessage) << "\r\n"
                                    << "Content-Type: " << "text/plain"
                                    << "\r\n\r\n" << errorMessage;
                    }else{
                        struct stat fileInfo;
                        if(fstat(fd, &fileInfo) == 0){
                            size_t size = fileInfo.st_size; 
                            httpResponse << "HTTP/1.1 200 OK\r\nContent-Length: " 
                                    << size << "\r\n"
                                    << "Content-Type: " << getMimeType(req.path) 
                                    << "\r\n\r\n";
                            std::string httpResponseStr = httpResponse.str();
                            size_t total_size = httpResponseStr.size();
                            size_t bytes_written = 0;
                            int eagainRetries = 10;
                            while(bytes_written < total_size){
                                ssize_t bytes_sent = write(clientFd, httpResponse.str().c_str() + bytes_written, total_size - bytes_written);
                                if(bytes_sent < 0){
                                    int err_code = errno; 
                                    if(err_code == EAGAIN){
                                        --eagainRetries;
                                        std::cerr << "EAGAIN Retry";
                                        if(eagainRetries <= 0){
                                            close(clientFd);
                                            {
                                            std::lock_guard<std::mutex> guard(connectionStatesMutex_);
                                            connectionStates_.erase(clientFd);
                                            }
                                            return false;
                                        }
                                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                                        continue;
                                    }
                                    else{
                                        std::cerr << "write failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";  
                                        close(clientFd);
                                        {
                                        std::lock_guard<std::mutex> guard(connectionStatesMutex_);
                                        connectionStates_.erase(clientFd);
                                        }
                                        return false; 
                                    }                            
                                    
                                }else{
                                    eagainRetries = 10;
                                }
                                if(bytes_sent == 0) break;
                                bytes_written += bytes_sent;
                            }
                            if(bytes_written >= total_size){
                                off_t offset = 0;
                                size_t remaining_bytes = size;
                                int eagainRetries = 500;
                                while(remaining_bytes > 0){
                                    ssize_t sent = sendfile(clientFd, fd, &offset, size);
                                    if(sent < 0){
                                        int err_code = errno; 
                                        if(err_code == EAGAIN){
                                            --eagainRetries;
                                            if(eagainRetries <= 0){
                                                close(clientFd);
                                                {
                                                std::lock_guard<std::mutex> guard(connectionStatesMutex_);
                                                connectionStates_.erase(clientFd);
                                                }
                                                return false;
                                            }
                                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                                            continue;
                                        }
                                        else {
                                            std::cerr << "sendfile failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                                            const char* errorMessage = "404 Error: File Not Found";
                                            httpResponse << "HTTP/1.1 404 NOT FOUND\r\nContent-Length: " 
                                                << strlen(errorMessage) << "\r\n"
                                                << "Content-Type: " << "text/plain"
                                                << "\r\n\r\n" << errorMessage;
                                            break;
                                        }
                                        
                                    }else{
                                        eagainRetries = 20;
                                    }
                                    remaining_bytes -= sent;
                                }
                                if(remaining_bytes <= 0) sentfile = true;

                                

                            }
                            if(size < cachingThreshold_){
                                std::ifstream file(req.path.substr(1));
                                if (!file.is_open()) {
                                    std::cerr << "file opening failed: " << req.path << " \n" << std::endl;
                                }
                                else{                        
                                    std::stringstream fileBuf;
                                    fileBuf << file.rdbuf();
                                    fileContents = fileBuf.str();
                                    cache_.put(req.path.substr(1), fileContents);
                                    
                                }
                            }
                            
                        }
                        else{
                            int err_code = errno; 
                            std::cerr << "fstat failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                            const char* errorMessage = "404 Error: File Not Found";
                            httpResponse << "HTTP/1.1 404 NOT FOUND\r\nContent-Length: " 
                                    << strlen(errorMessage) << "\r\n"
                                    << "Content-Type: " << "text/plain"
                                    << "\r\n\r\n" << errorMessage;
                        }
                    }
                    
                        
                    
                    
                    close(fd);
                    
                } 
                else{
                    fileContents = std::move(cacheResult.value());
                    httpResponse << "HTTP/1.1 200 OK\r\nContent-Length: " 
                                    << fileContents.size() << "\r\n"
                                    << "Content-Type: " << getMimeType(req.path) 
                                    << "\r\n\r\n" << fileContents;
                }
                
            
                
                if(!sentfile){
                    int bytes_sent = write(clientFd, httpResponse.str().c_str(), httpResponse.str().size());
                    if(bytes_sent < 0){
                        int err_code = errno; 
                        std::cerr << "write failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                    }
                }
                
            
            }
            else if(req.method == "POST"){
                // post handling
                std::string prefix = "/upload/";
         
                if(req.path.size() <= prefix.size() || !req.path.starts_with("/upload/")){
                    httpResponse << "HTTP/1.1 400 Bad Request\r\n"
                                    << "Content-Type: " << "text/plain"
                                    << "\r\n\r\n"
                                    << "Path must begin with /upload/";
                }
                else{
                    std::string filename = req.path.substr(std::string("/upload/").size());
                    size_t headerEnd = connState.buffer.find("\r\n\r\n");
                    std::string body = connState.buffer.substr(headerEnd + 4);  
                    int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                    if(fd < 0){
                        int err_code = errno; 
                        std::cerr << "open failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                        httpResponse << "HTTP/1.1 500 Internal Server Error\r\n"
                                    << "Content-Type: " << "text/plain"
                                    << "\r\n\r\n";
                    }
                    else{
                        size_t total_size = body.size();
                        size_t bytes_written = 0;
                        while(bytes_written < total_size){
                            ssize_t bytes_sent = write(fd, body.c_str() + bytes_written, total_size - bytes_written);
                            if(bytes_sent < 0){
                                int err_code = errno;
                                std::cerr << "write failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";  
                                httpResponse << "HTTP/1.1 500 Internal Server Error\r\n"
                                    << "Content-Type: " << "text/plain"
                                    << "\r\n\r\n";
                                break;
                            }
                            bytes_written += bytes_sent;
                        }
                        if(bytes_written >= total_size){
                            std::string hash = sha256Hex(body);
                            std::string message = "Uploaded " + filename + ", sha256: " + hash;
                            httpResponse << "HTTP/1.1 200 OK\r\n"
                                    << "Content-Length: " << message.size() << "\r\n"
                                    << "Content-Type: " << "text/plain"
                                    << "\r\n\r\n"
                                    << message << "\r\n";
                        }


                    }
                    close(fd);
                }

                int bytes_sent = write(clientFd, httpResponse.str().c_str(), httpResponse.str().size());
                if(bytes_sent < 0){
                    int err_code = errno; 
                    std::cerr << "write failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                }
                
            }else{
                
                close(clientFd);
                {
                std::lock_guard<std::mutex> guard(connectionStatesMutex_);
                connectionStates_.erase(clientFd);
                }
                
            }
            close(clientFd);
            {
            std::lock_guard<std::mutex> guard(connectionStatesMutex_);
            connectionStates_.erase(clientFd);
            }
            epoll_ctl(epollFd_, EPOLL_CTL_DEL, clientFd, nullptr);
            
        }   
        
        {   
            std::lock_guard lock(fdsMutex_);
            fdsBeingProcessed_.erase(clientFd);
        }
        
        return true;
        
        // std::cout << "Client disconnected" << std::endl;
        // close(clientFd);
    }

    

    ~Server(){
        running_ = false;
        close(fd_);
    }
private:
    int fd_;
    int epollFd_;
    sockaddr_in serverAddress_;

    struct epoll_event ev;
    struct epoll_event evList[MAX];

    bool running_;
    const size_t cachingThreshold_ = 5000000;

    std::unordered_set<int> fdsBeingProcessed_;
    ThreadPool threadPool_;
    std::mutex fdsMutex_;
    struct HTTPRequest{
        std::string method;
        std::string path;
        size_t contentLength;
    };
    struct ParseResult{
        int result; // error code. For now, 0 = success, -1 = failure.
        HTTPRequest req;
    };
    enum ConnectionStateType{
        Headers,
        Content,
        Complete
    };
    struct ConnectionState{
        std::string buffer;
        std::optional<size_t> contentLength;
        std::optional<HTTPRequest> http;
        ConnectionStateType state = Headers;
    };
    std::unordered_map<int, ConnectionState> connectionStates_;
    std::mutex connectionStatesMutex_;

    LRUCache cache_;

    
    struct FdGuard {
        std::unordered_set<int>& set;
        std::mutex& mtx;
        int fd;
        ~FdGuard() {
            std::lock_guard lock(mtx);
            set.erase(fd);
        }
    };

    struct ConnStateGuard {
        std::unordered_map<int, ConnectionState>& mp;
        std::mutex& mtx;
        int fd;
        ~ConnStateGuard() {
            std::lock_guard lock(mtx);
            mp.erase(fd);
        }
    };
    


    ParseResult parseHttp(char* buf, uint32_t len){
        std::string_view view(buf, len);
        std::vector<std::string_view> lines = split(view, "\r\n");

        if (lines.empty()) {
            return ParseResult{-1, HTTPRequest{"", "", 0}};
        }

        std::vector<std::string_view> firstLineTokens = split(lines[0], " ");

        if (firstLineTokens.size() != 3 ||
            firstLineTokens[1].empty() ||
            firstLineTokens[1][0] != '/') {
            return ParseResult{-1, HTTPRequest{"", "", 0}};
        }

        uint32_t contentLength = 0;


        for (size_t i = 1; i < lines.size(); ++i) {
            std::string_view line = lines[i];

            
            if (line.empty()) {
                break;
            }

            size_t colon = line.find(':');
            if (colon == std::string_view::npos) {
                continue;
            }

            std::string_view name = line.substr(0, colon);
            std::string_view value = line.substr(colon + 1);

   
            while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
                value.remove_prefix(1);
            }

            if (name == "Content-Length") {
                try {
                    contentLength = std::stoul(std::string(value));
                } catch (...) {
                    return ParseResult{-1, HTTPRequest{"", "", 0}};
                }
            }
        }

        HTTPRequest req {
            std::string(firstLineTokens[0]),
            std::string(firstLineTokens[1]),
            contentLength
        };

        return ParseResult{0, req};
    }
};


int main() {
    Server server(8080, 16, 1000);
    server.main();
    return 0;
}
