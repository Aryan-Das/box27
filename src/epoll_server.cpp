#include <iostream>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <stdexcept>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <unordered_map>

#include "utils.hpp"
#include "mime.hpp"

constexpr int MAX = 1000;


class Server{
public:
    Server(uint32_t port)
    : fd_ { socket(AF_INET, SOCK_STREAM, 0) }
    , epollFd_ {epoll_create1(0)}
    , serverAddress_ { }
    , ev { }
    , running_ { true }
    {
        serverAddress_.sin_family = AF_INET;
        serverAddress_.sin_port = htons(port);
        serverAddress_.sin_addr.s_addr = htonl(INADDR_ANY);

        if(bind(fd_, (struct sockaddr*)&serverAddress_, sizeof(serverAddress_)) < 0){
            int err_code = errno; 
            std::cerr << "bind failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
            throw std::runtime_error("Failed to bind socket");
        }
        if(listen(fd_, 10) < 0) {
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
                        if(!handleClient(evList[i].data.fd)){
                            epoll_ctl(epollFd_, EPOLL_CTL_DEL, evList[i].data.fd, &evList[i]);
                            close(evList[i].data.fd);
                            
                        }
                    }
                }
            }
        }
    }
    
    
    bool handleClient(int clientFd){
    
        char buffer[1024] = { 0 };
        int bytes_received = recv(clientFd, buffer, sizeof(buffer), 0);
        if(bytes_received < 0){
            int err_code = errno; 
            std::cerr << "recv failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
            return false;
        }
        else if(bytes_received == 0) return false;
        else{
            std::stringstream httpResponse; 
            ParseResult parsed = parseHttp(buffer, bytes_received);
            if(parsed.result < 0){
                std::cerr << "HTTP parse failed with error code: " << parsed.result << "\n";
                const char* errorMessage = "400 Error: Bad Request";
                httpResponse << "HTTP/1.1 400 BAD REQUEST\r\nContent-Length: "
                            << strlen(errorMessage) << "\r\n"
                            << "Content-Type: " << "text/plain" 
                            << "\r\n\r\n" 
                            << errorMessage;
            }
            else{
                HTTPRequest req = parsed.req;
                std::string test_print = "Method: " + req.method + "; Path: " + req.path;
                
                std::ifstream file(req.path.substr(1));
                if (!file.is_open()) {
                    std::cerr << "file opening failed: " << req.path << " \n" << std::endl;
                    const char* errorMessage = "404 Error: File Not Found";
                    httpResponse << "HTTP/1.1 404 NOT FOUND\r\nContent-Length: " 
                                << strlen(errorMessage) << "\r\n"
                                << "Content-Type: " << "text/plain"
                                << "\r\n\r\n" << errorMessage;
                }
                else{                        
                    std::stringstream fileBuf;
                    fileBuf << file.rdbuf();
                    std::string fileContents = fileBuf.str();
                    
                    httpResponse << "HTTP/1.1 200 OK\r\nContent-Length: " 
                                << fileContents.size() << "\r\n"
                                << "Content-Type: " << getMimeType(req.path) 
                                << "\r\n\r\n" << fileContents;
                    
                }
                
            }
            int bytes_sent = write(clientFd, httpResponse.str().c_str(), httpResponse.str().size());
            if(bytes_sent < 0){
                int err_code = errno; 
                std::cerr << "write failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
            }
            
            
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
   

    struct HTTPRequest{
        std::string method;
        std::string path;
    };
    struct ParseResult{
        int result; // error code. For now, 0 = success, -1 = failure.
        HTTPRequest req;
    };

    ParseResult parseHttp(char* buf, uint32_t len){
        std::string_view view = std::string_view(buf, len);
        std::vector<std::string_view> lines = split(view, "\r\n");
        std::vector<std::string_view> firstLineTokens = split(lines[0], " ");
        if (firstLineTokens.size() != 3 
            || view.find("\r\n") == std::string_view::npos
            || firstLineTokens[1][0] != '/'
        ){
            return ParseResult{-1, HTTPRequest{"", ""}};
        }
        HTTPRequest req {
            std::string(firstLineTokens[0]),
            std::string(firstLineTokens[1])
        };
        return ParseResult{0, req};
         
    }
};


int main() {
    Server server(8080);
    server.main();
    return 0;
}
