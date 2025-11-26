/*
* dfc.c
* assumptions -
* list: prints "<filename> not found" if file is not present in any server
* list: prints "No files found on any server corresponding to given command" if
no files found
* put: prints "File <filename> was uploaded successfully" if put is done
successfully
* get: prints "File <filename> successfully reconstructed" if get is done
successfully
* if file was chunked in different ways due to availability of servers
* - it will take the latest timestamp one
* - if timestamp is same it will take less total chunks one
* - have thought of scenario where same timestamp and same total chunks exist
but have not added logic for that
*/
#include <arpa/inet.h>

#include <errno.h>

#include <stdio.h>

#include <stdlib.h>

#include <string.h>

#include <sys/stat.h>

#include <sys/time.h>

#include <time.h>

#include <unistd.h>

#define DEBUG 0 // we can put this as 1 if we want to see the print statements
#define CONF_FILE "dfc.conf"
#define FILE_NAME_LIMIT 200
#define MAX_SERVERS 10
#define BUFFER_SIZE 8192
#define PATH_MAX 4096
typedef enum {
  LIST_CMD = 1,
    GET_CMD = 2,
    PUT_CMD = 3,
    GET_CHUNK_CMD = 4
}
command_type;
typedef struct {
  command_type cmd;
  char filename[FILE_NAME_LIMIT];
  int total_chunks;
  int chunk_id;
  int chunk_size;
  char client_name[100];
  time_t timestamp;
}
message_header;
typedef struct {
  char name[FILE_NAME_LIMIT];
  char ip_address[16];
  int port;
  int is_available;
}
server_data;
typedef struct {
  char filename[FILE_NAME_LIMIT];
  int total_chunks;
  int * chunks_found;
  time_t timestamp;
}
file_info; // for list operation
server_data servers[MAX_SERVERS];
int num_servers = 0;
void set_sock_timeout(int sock, int seconds);
ssize_t recv_data(int sock, void * buffer, size_t length);
ssize_t send_data(int sock,
  const void * buffer, size_t length);
void generate_random_string(char * str, int length);
int parse_config();
int check_server_availability();
int calculate_hash(const char * filename, char * hash_output, size_t hash_size);
void get_server_distribution(int hash_value, int chunk_id, int total_chunks,
  int * server1, int * server2);
int upload_chunk(int server_index,
  const char * filename, void * chunk_data,
    int chunk_id, int chunk_size, int total_chunks,
    time_t timestamp, char * random_str);
void handle_put_command(const char * filename);
void handle_get_command(const char * filename);
void handle_list_command(const char * filename);
int main(int argc, char * argv[]) {
    if (argc < 2) {
      fprintf(stderr,
        "Usage: %s <command> [filename] ... [filename] OR %s
        list\ n ",
        argv[0],
        argv[0]);
      return 1;
    }
    char * command = argv[1];
    if (parse_config() <= 0) {
      fprintf(stderr, "No valid servers found in config file\n");
      return 1;
    }
    if (strcmp(command, "list") == 0) {
      if (argc == 2) {
        handle_list_command(NULL); // list all command
      } else {
        for (int i = 2; i < argc; i++) {
          handle_list_command(argv[i]); // list <filename> command
        }
      }
      return 0;
    }
    if (argc < 3) {
      fprintf(stderr, "Usage: %s %s <filename> ... <filename>\n", argv[0],
        command);
      return 1;
    }
    for (int i = 2; i < argc; i++) {
      if (DEBUG == 1) printf("Processing file: %s\n", argv[i]);
      if (strcmp(command, "put") == 0) {
        handle_put_command(argv[i]);
      } else if (strcmp(command, "get") == 0) {
        handle_get_command(argv[i]);
      } else {
        printf("Unknown command requested. Supported commands (get, put, list)\
          n ");
          return 1;
        }
      }
      return 0;
    }
    void set_sock_timeout(int sock, int seconds) {
      struct timeval tv;
      tv.tv_sec = seconds;
      tv.tv_usec = 0;
      setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, & tv, sizeof(tv));
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, & tv, sizeof(tv));
    }
    ssize_t recv_data(int sock, void * buffer, size_t length) {
      size_t total_received = 0;
      set_sock_timeout(sock, 10);
      while (total_received < length) {
        size_t to_read = (length - total_received < BUFFER_SIZE) ?
          length - total_received :
          BUFFER_SIZE;
        ssize_t bytes_received =
          recv(sock, (char * ) buffer + total_received, to_read, 0);
        if (bytes_received < 0) {
          if (errno == EINTR) continue;
          if (DEBUG == 1) printf("Receive error: %s\n", strerror(errno));
          return -1;
        } else if (bytes_received == 0) {
          return total_received > 0 ? total_received : -1;
        }
        total_received += bytes_received;
      }
      return total_received;
    }
    ssize_t send_data(int sock,
      const void * buffer, size_t length) {
      size_t total_sent = 0;
      set_sock_timeout(sock, 10);
      while (total_sent < length) {
        size_t to_send =
          (length - total_sent < BUFFER_SIZE) ? length - total_sent : BUFFER_SIZE;
        ssize_t bytes_sent =
          send(sock, (const char * ) buffer + total_sent, to_send, 0);
        if (bytes_sent < 0) {
          if (errno == EINTR) continue;
          if (DEBUG == 1) printf("Send error: %s\n", strerror(errno));
          return -1;
        } else if (bytes_sent == 0) {
          return total_sent > 0 ? total_sent : -1;
        }
        total_sent += bytes_sent;
      }
      return total_sent;
    }
    void generate_random_string(char * str, int length) {
      static
      const char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
      int i;
      srand(time(NULL));
      for (i = 0; i < length; i++) {
        int index = rand() % (sizeof(charset) - 1);
        str[i] = charset[index];
      }
      str[length] = '\0'; // generate a random string sort of like uuid that we
      discussed in class
    }
    int parse_config() {
        const char * home = getenv("HOME");
        if (!home) {
          if (DEBUG == 1) fprintf(stderr, "env variable error for home dir\n");
          return -1;
        }
        char config_path[PATH_MAX];
        snprintf(config_path, sizeof(config_path), "%s/%s", home, CONF_FILE); // home
        dir location FILE * config_file = fopen(config_path, "r");
        if (!config_file) {
          if (DEBUG == 1) fprintf(stderr, "dfc.conf file error while open path: %
            s\ n ", config_path);
            return -1;
          }
          char line[256];
          char name[FILE_NAME_LIMIT], ip[16];
          int port;
          while (fgets(line, sizeof(line), config_file)) {
            if (sscanf(line, "server %s %[^:]:%d", name, ip, & port) == 3) {
              if (num_servers < MAX_SERVERS) {
                strcpy(servers[num_servers].name, name);
                strcpy(servers[num_servers].ip_address, ip);
                servers[num_servers].port = port;
                servers[num_servers].is_available = 0;
                num_servers++;
              } else {
                if (DEBUG == 1) printf("Maximum server limit reached\n");
                break;
              }
            }
          }
          fclose(config_file);
          return num_servers;
        }
        int check_server_availability() {
          int available_count = 0;
          for (int i = 0; i < num_servers; i++) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
              perror("socket creation error");
              continue;
            }
            set_sock_timeout(sock, 1); // 1 sec timeout
            struct sockaddr_in server_addr;
            memset( & server_addr, 0, sizeof(server_addr));
            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(servers[i].port);
            if (inet_pton(AF_INET, servers[i].ip_address, & server_addr.sin_addr) <= 0) {
              if (DEBUG == 1)
                fprintf(stderr, "Invalid ip address: %s\n", servers[i].ip_address);
              close(sock);
              continue;
            }
            if (connect(sock, (struct sockaddr * ) & server_addr, sizeof(server_addr)) <
              0) {
              if (DEBUG == 1)
                fprintf(stderr, "Server %s is unavailable\n", servers[i].name);
              servers[i].is_available = 0;
            } else {
              if (DEBUG == 1) printf("Server %s is available\n", servers[i].name);
              servers[i].is_available = 1;
              available_count++;
            }
            close(sock);
          }
          return available_count;
        }
        int calculate_hash(const char * filename, char * hash_output, size_t hash_size) {
          char md5_cmd[PATH_MAX + 10];
          char md5_result[128];
          snprintf(md5_cmd, sizeof(md5_cmd), "echo -n \"%s\" | md5sum",
            filename); // md5
          of filename using cmd FILE * fp = popen(md5_cmd, "r");
          if (fp == NULL) {
            perror("Failed to run md5sum command");
            return -1;
          }

          if (fgets(md5_result, sizeof(md5_result), fp) != NULL) {
            char * space_pos = strchr(md5_result, ' ');
            if (space_pos) {
              * space_pos = '\0';
              strncpy(hash_output, md5_result, hash_size - 1);
              hash_output[hash_size - 1] = '\0';
              pclose(fp);
              return 0;
            }
          }
          pclose(fp);
          return -1;
        }
        void get_server_distribution(int hash_value, int chunk_id, int total_chunks,
          int * server1, int * server2) {
          int available_servers[MAX_SERVERS];
          int available_count = 0;
          for (int i = 0; i < num_servers; i++) {
            if (servers[i].is_available) {
              available_servers[available_count++] = i;
            }
          }
          if (available_count < 2) {
            * server1 = * server2 = -1;
            return;
          }
          * server1 =
            available_servers[(hash_value + chunk_id - 1) % available_count]; //
          pair of chunk distributed * server2 =
            available_servers[(hash_value + chunk_id) % available_count];
        }
        int upload_chunk(int server_index,
            const char * filename, void * chunk_data,
              int chunk_id, int chunk_size, int total_chunks,
              time_t timestamp, char * random_str) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
              perror("Socket creation error");
              return -1;
            }
            set_sock_timeout(sock, 10);
            struct sockaddr_in server_addr;
            memset( & server_addr, 0, sizeof(server_addr));
            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(servers[server_index].port);
            if (inet_pton(AF_INET, servers[server_index].ip_address, &
                server_addr.sin_addr) <= 0) {
              perror("Invalid ip address");
              close(sock);
              return -1;
            }
            if (connect(sock, (struct sockaddr * ) & server_addr, sizeof(server_addr)) < 0) {
              perror("connect error");
              close(sock);
              return -1;
            }
            message_header header;
            memset( & header, 0, sizeof(header));
            header.cmd = PUT_CMD;
            strncpy(header.filename, filename, FILE_NAME_LIMIT - 1);
            header.total_chunks = total_chunks;
            header.chunk_id = chunk_id;
            header.chunk_size = chunk_size;
            snprintf(header.client_name, sizeof(header.client_name), "client%d%s",
              (int) getpid(), random_str); // client_name - clientPIDRandomString
            header.timestamp = timestamp;
            if (DEBUG == 1)
              printf("Sending chunk %d/%d of %s to server %s\n", chunk_id, total_chunks,
                filename, servers[server_index].name);
            if (send_data(sock, & header, sizeof(header)) != sizeof(header)) {
              perror("chunk header send error");
              close(sock);
              return -1;
            }
            if (send_data(sock, chunk_data, chunk_size) != chunk_size) {
              perror("chunk data send error");
              close(sock);
              return -1;
            }
            message_header ack_header;
            if (recv_data(sock, & ack_header, sizeof(ack_header)) != sizeof(ack_header)) {
              perror("recv ack error");
              close(sock);
              return -1;
            }
            close(sock);
            if (ack_header.chunk_size == -1) { // getting ack from server after done with
              put
              if (DEBUG == 1) fprintf(stderr, "error processsing chunk on server
                side\ n ");
                return -1;
              }
              if (DEBUG == 1)
                printf("Successfully uploaded chunk %d to server %s\n", chunk_id,
                  servers[server_index].name);
              return 0;
            }
            void handle_put_command(const char * filename) {
                struct stat fstat;
                if (stat(filename, & fstat) != 0) {
                  fprintf(stderr, "File access error for file: %s\n", filename);
                  return;
                }
                FILE * file = fopen(filename, "rb");
                if (!file) {
                  fprintf(stderr, "File open error for file: %s\n", filename);
                  return;
                }
                long file_size = fstat.st_size;
                int available_count = check_server_availability();
                if (available_count < 2) {
                  fprintf(stderr, "%s put failed\n", filename); // required at least 2
                  servers fclose(file);
                  return;
                }
                int total_chunks = available_count; // chunks dependent on number of servers
                available long chunk_size = (file_size + total_chunks - 1) / total_chunks;
                if (DEBUG == 1) printf("Chunking %ld byte file into %d chunks of %ld bytes
                    approx each\ n ", file_size, total_chunks, chunk_size);
                    char hash[100];
                    if (calculate_hash(filename, hash, sizeof(hash)) != 0) {
                      if (DEBUG == 1) fprintf(stderr, "Failed to calculate file hash\n");
                      fclose(file);
                      return;
                    }
                    if (DEBUG == 1) printf("File hash: %s\n", hash); unsigned long long hash_int = strtoull(hash, NULL, 16); // to make hash integer
                    to then get mod value int hash_value = hash_int % available_count;
                    if (DEBUG == 1) printf("Hash distribution value: %d\n", hash_value); char * chunk_buffer = malloc(chunk_size);
                    if (!chunk_buffer) {
                      perror("Memory allocation error");
                      fclose(file);
                      return;
                    }
                    struct timeval ts; gettimeofday( & ts, NULL); time_t timestamp = ts.tv_sec; int * chunk_success = calloc(total_chunks + 1, sizeof(int));
                    if (!chunk_success) {
                      perror("Memory allocation error");
                      free(chunk_buffer);
                      fclose(file);
                      return;
                    }

                    char random_str[6]; generate_random_string(random_str, 5);
                    for (int chunk_id = 1; chunk_id <= total_chunks; chunk_id++) {
                      int bytes_read;
                      long offset = (chunk_id - 1) * chunk_size;
                      fseek(file, offset, SEEK_SET);
                      long remaining_bytes = file_size - offset;
                      long this_chunk_size = (remaining_bytes >= chunk_size) ?
                        chunk_size :
                        (remaining_bytes > 0 ? remaining_bytes : 0);
                      if (this_chunk_size == 0) { // it happened for small chunks - no data was
                        left to put so sending empty buffer
                        if (DEBUG == 1)
                          fprintf(stderr, "Chunk %d has no data to read\n", chunk_id);
                        memset(chunk_buffer, 0, chunk_size);
                        bytes_read = 0;
                      } else {
                        bytes_read = fread(chunk_buffer, 1, chunk_size, file);
                        if (bytes_read <= 0) {
                          if (DEBUG == 1)
                            fprintf(stderr, "Read chunk error for chunk %d\n", chunk_id);
                          continue;
                        }
                      }
                      int server1, server2;
                      get_server_distribution(hash_value, chunk_id, total_chunks, & server1, &
                        server2);
                      if (server1 == -1 || server2 == -1) {
                        if (DEBUG == 1) fprintf(stderr, "cannot distribute chunks amongst the
                          servers\ n ");
                          continue;
                        }
                        if (DEBUG == 1)
                          printf("Chunk %d will be stored on servers %s and %s\n", chunk_id,
                            servers[server1].name, servers[server2].name);
                        int chunk1 =
                          upload_chunk(server1, filename, chunk_buffer, chunk_id, bytes_read,
                            total_chunks, timestamp, random_str);
                        int chunk2 =
                          upload_chunk(server2, filename, chunk_buffer, chunk_id, bytes_read,
                            total_chunks, timestamp, random_str);
                        if (chunk1 == 0 || chunk2 == 0) {
                          chunk_success[chunk_id] = 1;
                        }
                      }
                      int all_success = 1;
                      for (int i = 1; i <= total_chunks; i++) {
                        if (!chunk_success[i]) {
                          all_success = 0;
                          if (DEBUG == 1) fprintf(stderr, "chunk %d wass not uploaded to any
                            server\ n ", i);
                          }

                        }
                        if (all_success) {
                          printf("File %s was uploaded successfully\n", filename);
                        } else {
                          fprintf(stderr, "File %s was not uploaded or uploaded partially\n",
                            filename);
                        }
                        free(chunk_buffer);
                        free(chunk_success);
                        fclose(file);
                      }
                      void handle_get_command(const char * filename) {
                          int available_count = check_server_availability();
                          if (available_count < 2) {
                            fprintf(stderr, "%s is incomplete\n", filename);
                            return;
                          }
                          typedef struct {
                            char client_name[100];
                            time_t timestamp;
                            int chunk_id;
                            int total_chunks;
                            int server_index;
                            int received;
                            char * data;
                            int size;
                          }
                          chunk_info; // to help in reconstructing the file
                          chunk_info * chunks = NULL;
                          int chunks_count = 0;
                          int total_chunks = 0;
                          int latest_timestamp = 0;
                          for (int i = 0; i < num_servers; i++) {
                            int sock = socket(AF_INET, SOCK_STREAM, 0);
                            if (sock < 0) {
                              perror("socket creation error");
                              continue;
                            }
                            struct sockaddr_in server_addr;
                            memset( & server_addr, 0, sizeof(server_addr));
                            server_addr.sin_family = AF_INET;
                            server_addr.sin_port = htons(servers[i].port);
                            if (inet_pton(AF_INET, servers[i].ip_address, & server_addr.sin_addr) < 0) {
                              perror("Invalid ip address");
                              close(sock);
                              continue;
                            }
                            if (connect(sock, (struct sockaddr * ) & server_addr, sizeof(server_addr)) <
                              0) {
                              if (DEBUG == 1)
                                printf("connect error to server: %s:%d\n", servers[i].ip_address,
                                  servers[i].port);
                              close(sock);
                              continue;
                            }
                            message_header header = {
                              0
                            };
                            header.cmd = GET_CMD;
                            strncpy(header.filename, filename, sizeof(header.filename) - 1);
                            if (send_data(sock, & header, sizeof(header)) != sizeof(header)) {
                              perror("send get header error");
                              close(sock);
                              continue;
                            }
                            while (1) {
                              message_header response;
                              if (recv_data(sock, & response, sizeof(response)) != sizeof(response)) {
                                perror("receive error for get command");
                                break;
                              }
                              if (strcmp(response.filename, "DONE") == 0) {
                                break;
                              }
                              if (response.chunk_size == -1 ||
                                strcmp(response.filename, "ERROR") == 0) {
                                if (DEBUG == 1)("Server reported error");
                                break;
                              }
                              if (response.timestamp > latest_timestamp) {
                                latest_timestamp = response.timestamp;
                              }
                              if (total_chunks == 0) {
                                total_chunks = response.total_chunks;
                              } else if (total_chunks != response.total_chunks) {
                                if (DEBUG == 1) printf("number of total chunks from servers
                                  different\ n "); // TODO: improve this and add condition for client name as well
                                  if (response.total_chunks < total_chunks && latest_timestamp ==
                                    response.timestamp) {
                                    total_chunks = response.total_chunks;
                                  }
                                }
                                chunks = realloc(chunks, (chunks_count + 1) * sizeof(chunk_info));
                                if (!chunks) {
                                  perror("memory allocation error");
                                  close(sock);
                                  return;
                                }
                                chunk_info * new_chunk = & chunks[chunks_count];
                                memset(new_chunk, 0, sizeof(chunk_info));
                                strncpy(new_chunk -> client_name, response.client_name,
                                  sizeof(new_chunk -> client_name));
                                new_chunk -> timestamp = response.timestamp;
                                new_chunk -> chunk_id = response.chunk_id;

                                new_chunk -> total_chunks = response.total_chunks;
                                new_chunk -> server_index = i;
                                new_chunk -> received = 0;
                                new_chunk -> data = NULL;
                                chunks_count++;
                              }
                              close(sock);
                            }
                            if (chunks_count == 0) {
                              if (DEBUG == 1)
                                printf("No chunks found for file %s on any servers\n", filename);
                              return;
                            }
                            if (DEBUG == 1)
                              printf("Found %d chunks for file %s - Total required: %d\n", chunks_count,
                                filename, total_chunks);
                            int * chunk_availability = calloc(total_chunks, sizeof(int)); // keep track of
                            all available chunks
                            if (!chunk_availability) {
                              perror("memory allocation error");
                              free(chunks);
                              return;
                            }
                            for (int i = 0; i < chunks_count; i++) {
                              if (chunks[i].timestamp == latest_timestamp) {
                                if (chunks[i].chunk_id >= 1 && chunks[i].chunk_id <= total_chunks) {
                                  chunk_availability[chunks[i].chunk_id - 1]++;
                                }
                              }
                            }
                            int complete = 1;
                            for (int i = 0; i < total_chunks; i++) {
                              if (chunk_availability[i] == 0) {
                                complete = 0;
                                break;
                              }
                            }
                            if (!complete) { // TODO: retry mechanism maybe??
                              printf("%s is incomplete\n", filename);
                              free(chunk_availability);
                              free(chunks);
                              return;
                            }
                            for (int chunk_id = 1; chunk_id <= total_chunks; chunk_id++) {
                              int best_server = -1;
                              for (int i = 0; i < chunks_count; i++) {
                                if (chunks[i].chunk_id == chunk_id &&
                                  chunks[i].timestamp == latest_timestamp) {
                                  best_server = i;

                                  break;
                                }
                              }
                              if (best_server == -1) {
                                if (DEBUG == 1)
                                  printf("Could not find server for chunk %d\n",
                                    chunk_id); // chunks not found on any server suitable
                                continue;
                              }
                              chunk_info * current_chunk = & chunks[best_server];
                              int server_index = current_chunk -> server_index;
                              int sock = socket(AF_INET, SOCK_STREAM, 0);
                              if (sock < 0) {
                                perror("socket creation error");
                                continue;
                              }
                              struct sockaddr_in server_addr;
                              memset( & server_addr, 0, sizeof(server_addr));
                              server_addr.sin_family = AF_INET;
                              server_addr.sin_port = htons(servers[server_index].port);
                              if (inet_pton(AF_INET, servers[server_index].ip_address, &
                                  server_addr.sin_addr) < 0) {
                                perror("invalid ip address");
                                continue;
                              }
                              if (connect(sock, (struct sockaddr * ) & server_addr, sizeof(server_addr)) <
                                0) {
                                if (DEBUG == 1) printf("connect error - chunk download\n");
                                close(sock);
                                continue;
                              }
                              message_header request = {
                                0
                              };
                              request.cmd = GET_CHUNK_CMD;
                              request.chunk_id = current_chunk -> chunk_id;
                              request.total_chunks = current_chunk -> total_chunks;
                              request.timestamp = current_chunk -> timestamp;
                              strncpy(request.filename, filename, sizeof(request.filename) - 1);
                              strncpy(request.client_name, current_chunk -> client_name,
                                sizeof(request.client_name) - 1);
                              if (send_data(sock, & request, sizeof(request)) != sizeof(request)) { //
                                requesting required chunk one by one perror("send chunk request error");
                                close(sock);
                                continue;
                              }
                              message_header response;
                              if (recv_data(sock, & response, sizeof(response)) != sizeof(response)) {
                                perror("receive chunk header error");
                                close(sock);
                                continue;
                              }

                              if (response.chunk_size == -1 || strcmp(response.filename, "ERROR") == 0) {
                                if (DEBUG == 1) printf("Server reported error for chunk %d\n", chunk_id);
                                close(sock);
                                continue;
                              }
                              current_chunk -> data = malloc(response.chunk_size);
                              if (!current_chunk -> data) {
                                perror("memory allocation error for chunk data error");
                                close(sock);
                                continue;
                              }
                              current_chunk -> size = response.chunk_size;
                              if (recv_data(sock, current_chunk -> data, current_chunk -> size) !=
                                current_chunk -> size) {
                                perror("receive chunk data error");
                                free(current_chunk -> data);
                                current_chunk -> data = NULL;
                                close(sock);
                                continue;
                              }
                              current_chunk -> received = 1; // to keep track of all chunks
                              if (DEBUG == 1)
                                printf("Received chunk %d/%d of file %s\n", chunk_id, total_chunks,
                                  filename);
                              close(sock);
                            }
                            for (int chunk_id = 1; chunk_id <= total_chunks; chunk_id++) {
                              int found = 0;
                              for (int i = 0; i < chunks_count; i++) {
                                if (chunks[i].chunk_id == chunk_id && chunks[i].received) {
                                  found = 1;
                                  break;
                                }
                              }
                              if (!found) {
                                printf("%s is incomplete\n", filename);
                                for (int i = 0; i < chunks_count; i++) {
                                  if (chunks[i].data) {
                                    free(chunks[i].data);
                                  }
                                }
                                free(chunks);
                                free(chunk_availability);
                                return;
                              }
                            }
                            FILE * output_file = fopen(filename, "wb");
                            if (!output_file) {
                              perror("file creation error");
                              for (int i = 0; i < chunks_count; i++) {
                                if (chunks[i].data) {
                                  free(chunks[i].data);
                                }
                              }
                              free(chunks);
                              free(chunk_availability);
                              return;
                            }
                            for (int chunk_id = 1; chunk_id <= total_chunks; chunk_id++) {
                              for (int i = 0; i < chunks_count; i++) {
                                if (chunks[i].chunk_id == chunk_id && chunks[i].received) {
                                  if (fwrite(chunks[i].data, 1, chunks[i].size, output_file) !=
                                    chunks[i].size) { // write chunk data to file in cwd
                                    perror("file write error");
                                    fclose(output_file);
                                    for (int j = 0; j < chunks_count; j++) {
                                      if (chunks[j].data) {
                                        free(chunks[j].data);
                                      }
                                    }
                                    free(chunks);
                                    free(chunk_availability);
                                    return;
                                  }
                                  break;
                                }
                              }
                            }
                            fclose(output_file);
                            printf("File %s successfully reconstructed\n", filename);
                            for (int i = 0; i < chunks_count; i++) {
                              if (chunks[i].data) {
                                free(chunks[i].data);
                              }
                            }
                            free(chunks);
                            free(chunk_availability);
                          }
                          void handle_list_command(const char * filename) {
                              file_info * files = NULL;
                              int files_count = 0;
                              for (int i = 0; i < num_servers; i++) {
                                int sock = socket(AF_INET, SOCK_STREAM, 0);
                                if (sock < 0) {
                                  perror("socket creation error");
                                  continue;
                                }
                                struct sockaddr_in server_addr;
                                memset( & server_addr, 0, sizeof(server_addr));
                                server_addr.sin_family = AF_INET;
                                server_addr.sin_port = htons(servers[i].port);
                                if (inet_pton(AF_INET, servers[i].ip_address, & server_addr.sin_addr) < 0) {
                                  perror("ip address invalid");
                                  close(sock);
                                  continue;
                                }
                                if (connect(sock, (struct sockaddr * ) & server_addr, sizeof(server_addr)) <
                                  0) {
                                  if (DEBUG == 1)
                                    printf("connect error for server: %s:%d\n", servers[i].ip_address,
                                      servers[i].port);
                                  close(sock);
                                  continue;
                                }
                                message_header request = {
                                  0
                                };
                                request.cmd = LIST_CMD;
                                if (filename) {
                                  strncpy(request.filename, filename, sizeof(request.filename) - 1);
                                }
                                if (send_data(sock, & request, sizeof(request)) != sizeof(request)) { //
                                  request to all available servers perror("send list request error");
                                  close(sock);
                                  continue;
                                }
                                while (1) {
                                  message_header response; // get all chunk saved on servers
                                  if (recv_data(sock, & response, sizeof(response)) != sizeof(response)) {
                                    perror("receive error for list command");
                                    break;
                                  }
                                  if (strcmp(response.filename, "DONE") == 0) { // ack for when one file
                                    chunks are complete
                                    break;
                                  }
                                  if (response.chunk_size == -1 ||
                                    strcmp(response.filename, "ERROR") == 0) {
                                    if (DEBUG == 1) printf("Server reported error");
                                    break;
                                  }
                                  int file_index = -1;
                                  for (int j = 0; j < files_count; j++) {
                                    if (strcmp(files[j].filename, response.filename) == 0) {
                                      file_index = j;
                                      break;
                                    }
                                  }
                                  if (file_index == -1) {
                                    files = realloc(files, (files_count + 1) * sizeof(file_info));
                                    if (!files) {
                                      perror("memory allocation error");
                                      close(sock);
                                      return;
                                    }
                                    file_index = files_count++;
                                    memset( & files[file_index], 0, sizeof(file_info));

                                    strncpy(files[file_index].filename, response.filename,
                                      sizeof(files[file_index].filename) - 1);
                                    files[file_index].total_chunks = response.total_chunks;
                                    files[file_index].timestamp = response.timestamp;
                                    files[file_index].chunks_found =
                                      calloc(response.total_chunks, sizeof(int));
                                    if (!files[file_index].chunks_found) {
                                      perror("memory allocation error");
                                      close(sock);
                                      for (int k = 0; k < files_count - 1; k++) {
                                        free(files[k].chunks_found);
                                      }
                                      free(files);
                                      return;
                                    }
                                  }
                                  if (response.timestamp > files[file_index].timestamp) { // get the
                                    latest copy in
                                      case of duplicate files
                                    files[file_index].timestamp = response.timestamp;
                                    files[file_index].total_chunks = response.total_chunks;
                                    free(files[file_index].chunks_found);
                                    files[file_index].chunks_found = calloc(response.total_chunks,
                                      sizeof(int));
                                    if (!files[file_index].chunks_found) {
                                      perror("memory allocation error");
                                      close(sock);
                                      for (int k = 0; k < files_count; k++) {
                                        if (k != file_index && files[k].chunks_found) {
                                          free(files[k].chunks_found);
                                        }
                                      }
                                      free(files);
                                      return;
                                    }
                                    memset(files[file_index].chunks_found, 0, response.total_chunks *
                                      sizeof(int));
                                  }
                                  if (response.timestamp == files[file_index].timestamp) {
                                    if (response.chunk_id >= 1 && response.chunk_id <=
                                      files[file_index].total_chunks) {
                                      files[file_index].chunks_found[response.chunk_id - 1] = 1;
                                    }
                                  }
                                }
                                close(sock);
                              }
                              if (files_count == 0) {
                                if (filename) printf("%s not found\n", filename);
                                else printf("No files found on any server corresponding to given
                                  command\ n ");
                                  return;

                                }
                                for (int i = 0; i < files_count; i++) {
                                  int complete = 1;
                                  for (int j = 0; j < files[i].total_chunks; j++) {
                                    if (files[i].chunks_found[j] == 0) {
                                      complete = 0; // if any chunk is missing - mark as incomplete
                                      break;
                                    }
                                  }
                                  int available_chunks = 0;
                                  for (int j = 0; j < files[i].total_chunks; j++) {
                                    if (files[i].chunks_found[j]) {
                                      available_chunks++;
                                    }
                                  }
                                  printf("%s %s\n", files[i].filename, complete ? "" : "[incomplete]");
                                }
                                for (int i = 0; i < files_count; i++) {
                                  if (files[i].chunks_found) {
                                    free(files[i].chunks_found);
                                  }
                                }
                                free(files);
                              }
