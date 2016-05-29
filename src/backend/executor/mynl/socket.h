#ifndef HEAD_SOCKETFUNC__
#define HEAD_SOCKETFUNC__

#include <stdio.h>
#include <stdlib.h>
//#include <sys/types.h>
#include <sys/socket.h>
//#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>


/********** socket related functions **********/
static inline 
struct in_addr
getServerAddrSock(const int sock) {
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
	
        getsockname(sock, (struct sockaddr *)&addr, &len);
	
        return addr.sin_addr;
}

static inline 
in_port_t
getServerPortSock(const int sock) {
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
	
        getsockname(sock, (struct sockaddr *)&addr, &len);
	
        return addr.sin_port;
}

static inline 
struct in_addr
getPeerAddrSock(const int sock) {
        struct sockaddr_in addr;
	socklen_t len = sizeof(addr);
        
	getpeername(sock, (struct sockaddr *)&addr, &len);
	
        return addr.sin_addr;
}

static inline 
in_port_t
getPeerPortSock(const int sock) {
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
	
        getpeername(sock, (struct sockaddr *)&addr, &len);

        return addr.sin_port;
}

static inline 
int
setOptionSock(const int sock) {
        int optval = 1;
	
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
                fprintf(stderr, "error in setOptionSock::setsockopt()\n");
		return -1;
	}
	
	return 0;
}


static inline 
int
listenSock(const int port) {
	int sock;
        const int BACKLOG = 30;
        struct sockaddr_in addr;
        socklen_t addr_size;
	
	addr_size = sizeof(addr);
        bzero((char *)&addr, sizeof(addr));
	addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);
	
        if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "error in listenSock::socket()\n");
		return -1;
	}
        if (setOptionSock(sock) < 0)
                return -1;
        if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		fprintf(stderr, "error in listenSock::bind()\n");
		return -1;
	}
	
        if (listen(sock, BACKLOG) < 0) {
		fprintf(stderr, "error in listenSock::listen()\n");
                return -1;
	}

        return sock;
}

static inline 
int 
acceptSock(const int server_sock) {
        int client_sock;
        struct sockaddr_in addr;
        socklen_t addr_size = sizeof(addr);
	
        client_sock = accept(server_sock, (struct sockaddr *)&addr, &addr_size);
        if (client_sock < 0) {
		fprintf(stderr, "error in acceptSock::accept()\n");
		return -1;
	}

	return client_sock;
}


static inline
int
connectSock(const char * const address, const int port)
{
	int sock;
	struct addrinfo hints;
	struct addrinfo *list, *lc;
		
	
	bzero((char *)&hints, sizeof(hints));
	hints.ai_family = AF_INET; // PF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = 0;
	hints.ai_protocol = 0; // IPPROTO_TCP; 
	
	
	if (getaddrinfo(address, NULL, &hints, &list) != 0) {
		fprintf(stderr, "error in connectSock::getaddrinfo(%s)\n", address);
                return -1;
        }
	
	for (lc = list; lc != NULL; lc = lc->ai_next) {	
		if ((sock = socket(lc->ai_family, lc->ai_socktype, lc->ai_protocol)) < 0)
			continue;
		
		((struct sockaddr_in *)(lc->ai_addr))->sin_port = htons(port);
		if (connect(sock, lc->ai_addr, lc->ai_addrlen) < 0) {
			close(sock);
			sock = -1;
			continue;
		}
		
		break;
	}
	
	if (list != NULL)
		freeaddrinfo(list);
	
	if (lc == NULL) {
		fprintf(stderr, "error in connectSock--could not connect--\n");
                return -1;
	}
	
        return sock;
}

static inline
void
closeSock(const int sock)
{
	close(sock);
}


static inline
long
sendData(const int sock, const void *const data, const long total_byte) {
        long cumulative_byte = 0;

        while (true) {
                long send_byte = send(sock, (void *)((char *)data + cumulative_byte), total_byte - cumulative_byte, 0);
                if (send_byte == 0)
                        break;

                cumulative_byte += send_byte;
                if (cumulative_byte == total_byte)
                        break;

                else if (send_byte < 0)
                        return -1;
        }

        return cumulative_byte;
}


static inline 
long
recvData(const int sock, void *const buf, const long total_byte) {
        long cumulative_byte = 0;
	
	while (true) {
		long rec_byte = recv(sock, (void *)((char *)buf + cumulative_byte), total_byte - cumulative_byte, 0);
		if (rec_byte == 0)
			break;
		
		cumulative_byte += rec_byte;
		if (cumulative_byte == total_byte)
			break;
		
		else if (rec_byte < 0)
			return -1;
	}

        return cumulative_byte;
}

#endif // HEAD_SOCKETFUNC__
