#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>

#define MAX_EVENTS 10
#define BUF_SIZE 4096
#define REDIS_PONG "+PONG\r\n"

enum REDIS_DATA_IDENTIFIER {
	BULK_STRING = '$'
};

int set_nonblocking(int sockfd) {
	int flags, result;
	flags = fcntl(sockfd, F_GETFL, 0);
	if (flags == -1) {
		perror("fcntl - F_GETFL");
		return -1;
	}

	flags |= O_NONBLOCK;
	result = fcntl(sockfd, F_SETFL, flags);
	if (result == -1) {
		perror("fcntl - F_SETFL");
		return -1;
	}

	return 0;
}

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

	printf("Waiting for a client to connect...\n");

	int epoll_fd = epoll_create1(0);
	if (epoll_fd == -1) {
		perror("epoll_create");
		exit(1);
	}

	// add server fd to epoll list
	ev.events = EPOLLIN;
	ev.data.fd = server_fd;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
		perror("epoll_ctl: server_fd");
		exit(1);
	}

	client_addr_len = sizeof(client_addr);
	int running = 1;
	while (running) {
		nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
		if (nfds == -1) {
			perror("epoll_wait");
			exit(1);
		}

		int n;
		for (n = 0; n < nfds; n++) {
			// add new connections to epoll list
			if (events[n].data.fd == server_fd) {
				client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
				if (client_fd == -1) {
					if (errno == EAGAIN || errno == EWOULDBLOCK) {
						continue;
					}
					perror("accept");
					exit(1);
				}
				set_nonblocking(client_fd);
				ev.events = EPOLLIN | EPOLLET;
				ev.data.fd = client_fd;
				if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
					perror("epoll_ctl: client_fd");
					exit(1);
				}
				printf("Client connected! FD - %d\n", client_fd);
			}
			else {
				// socket available for reading
				if (events[n].events == EPOLLIN) {
					char buf[BUF_SIZE];
					ssize_t bytes_received;
					// Receive bytes
					// only read BUF_SIZE - 1 bytes -> last byte for '\0'
					bytes_received = recv(events[n].data.fd, buf, BUF_SIZE - 1, 0);
					if (bytes_received == -1) {
						perror("recv");
						exit(1);
					}
					// client connection closed
					if (bytes_received == 0) {
						printf("Client disconnected! FD - %d\n", events[n].data.fd);
						if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, events[n].data.fd, NULL) == -1) {
							perror("epoll_ctl: client_fd");
							exit(1);
						}
						continue;
					}
					buf[bytes_received] = '\0';

					char identifier = buf[0];
					switch (identifier) {
						case BULK_STRING:
							printf("Bulk string\n");
							break;
						default:
							printf("Default\n");
							break;
					}
					
					// change event type -> ready to write data
					ev.events = EPOLLOUT;
					ev.data.fd = events[n].data.fd;
					if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, events[n].data.fd, &ev) == -1) {
						perror("epoll_ctl: client_fd");
						exit(1);
					}
				}
				// socket available for writing
				else if (events[n].events == EPOLLOUT) {
					send(events[n].data.fd, REDIS_PONG, strlen(REDIS_PONG), 0);
					
					// switch conn back to reading -> persistent connections
					ev.events = EPOLLIN | EPOLLET;
					ev.data.fd = events[n].data.fd;
					if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, events[n].data.fd, &ev) == -1) {
						perror("epoll_ctl: client_fd");
						exit(1);
					}
				}
			}
		}
	}

	close(client_fd);
	close(server_fd);

	return 0;
}
