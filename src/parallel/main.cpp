
/* 
 * tcpserver.c - A multithreaded TCP echo server 
 * usage: tcpserver <port>
 * 
 * Testing : 
 * nc localhost <port> < input.txt
 */
#define TCP_HEADER
#include<bits/stdc++.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
using namespace std;

class Server{
    public:
        int po,si;
        string IP;
        Server(int port, int size){
            po = port;
            si = size;
            req_m = PTHREAD_MUTEX_INITIALIZER;
            kv_m = PTHREAD_MUTEX_INITIALIZER;
            client_m = PTHREAD_MUTEX_INITIALIZER;
            threads = (pthread_t*)malloc(sizeof(pthread_t)*si);
            sockaddr_in server_addr;
            server_addr.sin_port = htons(po);
            server_addr.sin_family = AF_INET;
            server_addr.sin_addr.s_addr = INADDR_ANY;

            sockid = socket(AF_INET, SOCK_STREAM, 0);
            if(bind(sockid, (sockaddr*)&server_addr, sizeof(server_addr)) < 0){
                cout << "Error is alredy in use" << endl;
                cerr << "Error " << strerror(errno) << endl;
                exit(0); 
            }

            if(listen(sockid, 5) < 0){
                cerr << "Error Listening" << endl;
                exit(0);
            }
            for(int i=0; i<si; i++){
                if(pthread_create(threads+i, NULL, &Server::serve_helper, this) < 0){
                    cerr << "Error in creating thread" << endl;
                    
                    exit(0);
                }
            }
            Add_request();
            for(int i=0; i<si; i++){
                pthread_join(threads[i], NULL);
            }
        }

        int Add_request(){
            int conn;
            char buffer[buffsize];
            while(1){
                conn = accept(sockid, nullptr, nullptr); 
                if(conn < 0){
                    cout << "Error Connecting to client" << endl;
                    exit(0);
                }
                pthread_mutex_lock(&client_m);
                clients.push(conn);
                pthread_mutex_unlock(&client_m);
                cout << "New Client" << endl;
            }
        }

    private:
        unordered_map<string, string> kv_store;
        queue<int> clients;
        pthread_mutex_t req_m, kv_m, client_m;
        pthread_t* threads;
        
        void parse_command(char* buffer, queue<string>& reqs){
            char* p = buffer;
            int mode = 0; 
            int i=0;
            while(*p && i<buffsize){
                string temp = "";
                while(*p != '\n'){
                    i++;
                    temp += *p;
                    p++;
                }
                p++;
                i++;
                if(temp == "READ"){
                    mode = 1;
                    temp = "1";
                }
                else if(temp == "WRITE"){
                    mode = 2;
                    temp = "2";
                }
                else if(temp == "COUNT"){
                    mode = 3;
                    temp = "3";
                }
                else if(temp == "DELETE"){
                    mode = 4;
                    temp = "4";
                }
                else if(temp == "END"){
                    mode = 5;
                    temp = "5";
                }
                if(mode == 1 || mode == 4){
                    temp += ' ';
                    while(*p != '\n'){
                        i++;
                        temp += *p;
                        p++;
                    }
                    p++;
                    i++;
                }
                if(mode == 2){
                    temp += ' ';
                    while(*p != '\n'){
                        i++;
                        temp += *p;
                        p++;
                    }
                    p++;
                    i++;
                    while(*p != '\n'){
                        i++;
                        temp += *p;
                        p++;
                    }
                    p++;
                    i++;
                }
                reqs.push(temp); 
                
            }
            buffer[i] = 0;
            pthread_mutex_unlock(&req_m);
            
        }
        void execution(queue<string>& reqs, int soc){
            string temp,keyval,value;
            int sock;
            while(!reqs.empty()){
                temp = reqs.front();
                reqs.pop();
                pthread_mutex_lock(&kv_m);
                sock = soc;
                if(temp[0] == '1'){
                    keyval = temp.substr(1, temp.size() -1);
                    cout << "read " << "\n" << keyval << endl;
                    if(kv_store.find(keyval) == kv_store.end()){
                        write(sock, "NULL\n", 5);
                    }
                    else write(sock, (kv_store[keyval]+"\n").c_str(), kv_store[keyval].size()+1);

                }
                else if(temp[0] == '2'){
                    size_t idx = temp.find(':');
                    keyval = temp.substr(1, idx-1);
                    value = temp.substr(idx+1, temp.size() - idx -1);
                    cout << "write " << "\n" << keyval << "\n" << value << endl;
                    kv_store[keyval] = value;
                    write(sock, "FIN\n", 4);
                }

                else if(temp[0] == '3'){
                    cout << "count " << endl;
                    write(sock, (to_string(kv_store.size())+"\n").c_str(), (to_string(kv_store.size())).size()+1);
                }
                else if(temp[0] == '4'){
                    keyval = temp.substr(1, temp.size() - 1);
                    cout << "delete " << "\n" << keyval << endl;
                    if(kv_store.find(keyval) == kv_store.end()){
                        write(sock, "NULL\n", 5);
                    }
                    else write(sock, "FIN\n", 4);
                }
                else{
                    cout << "end " << endl;
                    write(sock, "\n", 1);
                    close(sock);
                }
                pthread_mutex_unlock(&kv_m);
            }
            
        }

        void serve_client(){
            char buffer[buffsize];
            int conn;
            queue<string> requests;
            while(1){
                pthread_mutex_lock(&client_m);
                if(clients.size() <= 0){
                    pthread_mutex_unlock(&client_m);
                    continue;
                }
                cout << "Got a client" << endl;
                conn = clients.front();
                clients.pop();
                pthread_mutex_unlock(&client_m);
                while(fcntl(conn, F_GETFD) != -1){
                    memset(buffer, 0, buffsize);
                    read(conn, buffer, buffsize);
                    parse_command(buffer, requests);
                    execution(requests, conn);
                }
            }
        }

        static void* serve_helper(void* context){
            Server* c = static_cast<Server*>(context);
            c->serve_client();
            return nullptr;
        }

        int sockid;
        int buffsize = 4096;
};



int main(int argc, char ** argv) {
  int portno; /* port to listen on */
  
  /* 
   * check command line arguments 
   */
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  portno = atoi(argv[1]);
  Server server(portno,4);

}
