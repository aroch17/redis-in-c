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
#include <limits.h>
#include <ctype.h>
#include <search.h>

#define MAX_EVENTS 10
#define MAX_NUM_ELEM 128
#define BUF_SIZE 4096
#define DELIMITER_LEN 2
#define IDENTIFIER_LEN 1
#define NULL_TERMINATOR_LEN 1
#define REDIS_PONG "+PONG\r\n"
#define REDIS_NULL_STRING "-1\r\n"
#define REDIS_OK "+OK\r\n"

enum REDIS_DATA_IDENTIFIER {
	REDIS_BULK_STRING = '$',
	REDIS_ARRAY = '*'
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

/*
Input: Expects a RESP encoded bulk string, can supply multiple bulk strings chained together - parses the first one
Output: Parsed string contents - allocated on heap
*/
char* parse_bulk_string(char* buf) {
	int len = strtol(buf + IDENTIFIER_LEN, NULL, 10);
	if ((errno == ERANGE && (len == LONG_MAX || len == LONG_MIN)) || (errno != 0 && len == 0)) {
		perror("strtol");
		return NULL;
	}

	char* ret = malloc(len + NULL_TERMINATOR_LEN);
	if (ret == NULL) {
		printf("Failed to allocate memory\n");
		return NULL;
	}

	// find first occurence of \r\n
	char* ptr = strstr(buf, "\r\n"); 
	if (ptr == NULL) {
		printf("Bad string\n");
		return NULL;
	}
	ptr += DELIMITER_LEN;

	strncpy(ret, ptr, len);
	ret[len] = '\0';
	
	return ret;
}

void free_array_contents(char** array) {
	int i = 0;
	while (array[i] != NULL) {
		free(array[i]);
		i += 1;
	}
	free(array);
}

/*
Input: Expects a RESP encoded array
Output: Parsed array contents - double pointer allocated on heap
*/
char** parse_resp_array(char* buf) {
	int num_items = strtol(buf + IDENTIFIER_LEN, NULL, 10);
	if ((errno == ERANGE && (num_items == LONG_MAX || num_items == LONG_MIN)) || (errno != 0 && num_items == 0)) {
		perror("strtol");
		return NULL;
	}

	char** ret = malloc(sizeof(char*) * (num_items + NULL_TERMINATOR_LEN));
	// find starting positon of bulk string
	char* current_bulk_str = strchr(buf, '$');
	if (current_bulk_str == NULL) {
		printf("Invalid input\n");
		return NULL;
	}

	int i;
	for (i = 0; i < num_items; i++) {
		ret[i] = parse_bulk_string(current_bulk_str);
		if (ret[i] == NULL) {
			printf("Failed to parse array\n");
			free_array_contents(ret);
			return NULL;
		}

		// last item will not have any more strings after it
		if (i < num_items - 1) {
			current_bulk_str = strchr(current_bulk_str + IDENTIFIER_LEN, '$');
			if (current_bulk_str == NULL) {
				printf("Invalid input\n");
				return NULL;
			}
		}
	}
	ret[num_items] = NULL;
	return ret;
}

/*
Input - String to be encoded
				Length of string to be encoded
Output - RESP encoded string - heap allocated
				 Returns RESP2 NULL string on error
*/
char* encode_bulk_string(char* str, size_t len_str) {
	// special case if len_str == 0 -> return RESP2 NULL string
	// strndup as user expects heap allocated string
	if (len_str == 0) {
		return strndup(REDIS_NULL_STRING, strlen(REDIS_NULL_STRING));
	}

	if (str == NULL) {
		printf("Invalid str\n");
		return NULL;
	}

	int len_digits;
	if ((len_digits = snprintf(NULL, 0, "%zu", len_str)) < 0) {
		printf("Failed to calculate digit len\n");
		return NULL;
	}

	size_t encoded_str_len = (IDENTIFIER_LEN + len_digits) + len_str + (2 * DELIMITER_LEN) + NULL_TERMINATOR_LEN;
	char* encoded_str = malloc(encoded_str_len);
	if (encoded_str == NULL) {
		printf("Failed to allocate memory\n");
		return NULL;
	}

	int bytes_written;
	if ((bytes_written = snprintf(encoded_str, encoded_str_len, "$%zu\r\n%s\r\n", len_str, str)) < 0) {
		printf("Failed to encode\n");
		free(encoded_str);
		return NULL;
	}

	return encoded_str;
}

char* process_resp_array(char* buf) {
	char** items = parse_resp_array(buf);
	if (items == NULL) {
		return NULL;
	}
	char* cmd = items[0];
	
	char* resp;
	if (!strncasecmp(cmd, "PING", strlen(cmd))) {
		resp = strdup(REDIS_PONG);
	}
	else if (!strncasecmp(cmd, "ECHO", strlen(cmd))) {
		resp = encode_bulk_string(items[1], strlen(items[1]));
	}
	else if (!strncasecmp(cmd, "SET", strlen(cmd))) {
		if (!items[1] || !items[2]) {
			printf("Incomplete command\n");
			return NULL;
		}

		ENTRY e, *ep;
		e.key = items[1];
		e.data = items[2];
		ep = hsearch(e, ENTER);
		if (ep == NULL && errno == ENOMEM) {
			printf("Max elements reached\n");
			return NULL;
		}
		
		if (ep == NULL) {
			printf("Failed to create entry\n");
			return NULL;
		}
		
		resp = strdup(REDIS_OK);
	}
	else {
		printf("Invalid command\n");
		resp = NULL;
	}

	free_array_contents(items);
	return resp;
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
	char* resps[MAX_EVENTS];
	// create key-value store
	hcreate(MAX_NUM_ELEM);
	
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
					bytes_received = recv(events[n].data.fd, buf, BUF_SIZE - NULL_TERMINATOR_LEN, 0);
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
						case REDIS_ARRAY:
							if ((resps[n] = process_resp_array(buf)) == NULL) {
								resps[n] = strdup(REDIS_NULL_STRING);
							}
							break;
						default:
							printf("Invalid command\n");
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
					send(events[n].data.fd, resps[n], strlen(resps[n]), 0);
					
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

	hdestroy();
	close(client_fd);
	close(server_fd);

	return 0;
}