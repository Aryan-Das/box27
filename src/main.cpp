#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <stdexcept>

class Server{
public:
    Server(uint32_t port)
    : fd_ { socket(AF_INET, SOCK_STREAM, 0) }
    , serverAddress_ { }
    , running { true }
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
        
    }

    void acceptNewClient(){
        std::cout << "accepting connection"<< std::endl;
        int clientFd = accept(fd_, nullptr, nullptr);
        if(clientFd < 0){
            int err_code = errno; 
            std::cerr << "accept failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
        }else{
            std::cout << "accepted"<< std::endl;
            handleClient(clientFd);
        }
        
    }
    
    void handleClient(int clientFd){
        while(true){
            char buffer[1024] = { 0 };
            int bytes_received = recv(clientFd, buffer, sizeof(buffer), 0);
            if(bytes_received < 0){
                int err_code = errno; 
                std::cerr << "recv failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                break;
            }
            else if(bytes_received == 0) break;
            else{
                int bytes_sent = write(clientFd, buffer, bytes_received);
                if(bytes_sent < 0){
                    int err_code = errno; 
                    std::cerr << "write failed with error: " << std::strerror(err_code) << " (code: " << err_code << ")\n";
                }
            }   
            
            
        }
        std::cout << "Client disconnected" << std::endl;
        close(clientFd);
    }


    ~Server(){
        running = false;
        close(fd_);
    }
private:
    int fd_;
    sockaddr_in serverAddress_;
    bool running;
};


int main() {
    Server server(8080);
    while(true){
        server.acceptNewClient();
        
    }
    return 0;
}
