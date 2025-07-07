#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/epoll.h>

#define MAX_EVENTS 10
#define BUF_SIZE 4096
#define REDIS_PONG "+PONG\r\n"

int main() {
	// Disable output buffering
	setbuf(stdout, NULL);
	setbuf(stderr, NULL);
	
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	printf("Logs from your program will appear here!\n");
	
	int server_fd, client_addr_len, client_fd, nfds;
	struct sockaddr_in client_addr;
	struct epoll_event ev, events[MAX_EVENTS];
	
	server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd == -1) {
		printf("Socket creation failed: %s...\n", strerror(errno));
		return 1;
	}
	
	// Since the tester restarts the program quite often, setting SO_REUSEADDR
	// ensures that we don't run into 'Address already in use' errors
	int reuse = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
		printf("SO_REUSEADDR failed: %s \n", strerror(errno));
		return 1;
	}
	
	struct sockaddr_in serv_addr = { .sin_family = AF_INET ,
									 .sin_port = htons(6379),
									 .sin_addr = { htonl(INADDR_ANY) },
									};
	
	if (bind(server_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) != 0) {
		printf("Bind failed: %s \n", strerror(errno));
		return 1;
	}
	
	int connection_backlog = 5;
	if (listen(server_fd, connection_backlog) != 0) {
		printf("Listen failed: %s \n", strerror(errno));
		return 1;
	}

	int epoll_fd = epoll_create1(0);
	if (epoll_fd == -1) {
		perror("epoll_create");
		exit(1);
	}

	ev.events = EPOLLIN;
	ev.data.fd = server_fd;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
		perror("epoll_ctl: server_fd");
		exit(1);
	}
	
	printf("Waiting for a client to connect...\n");
	client_addr_len = sizeof(client_addr);
	
	client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
	printf("Client connected\n");

	char buf[BUF_SIZE];
	ssize_t bytes_received;

	int running = 1;
	while (running) {
		// Receive bytes
		// only read BUF_SIZE - 1 bytes -> last byte for '\0'
		bytes_received = recv(client_fd, buf, BUF_SIZE - 1, 0);
		if (bytes_received == -1) {
			perror("recv");
			exit(1);
		}
		
		buf[bytes_received] = '\0';
	
		if (!strcmp(buf, "*1\r\n$4\r\nPING\r\n")) {
			send(client_fd, REDIS_PONG, strlen(REDIS_PONG), 0);
		}
	}

	close(client_fd);
	close(server_fd);

	return 0;
}
