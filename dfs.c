/*
 * dfs.c - has multithreading for concurrent clients
 */
#include <stdio.h>

#include <string.h>

#include <stdlib.h>

#include <arpa/inet.h>

#include <signal.h>

#include <dirent.h>

#include <sys/stat.h>

#include <sys/time.h>

#include <errno.h>

#include <unistd.h>

#include <pthread.h>

#include <libgen.h> // this one i had to add for extracting filename if directory

was passed e.g.. / test_files / abc.txt
#define DEBUG 0 // we can put this as 1 if we want to see the print statements
#define FILE_NAME_LIMIT 200
#define PATH_MAX 4096
#define BUFFER_SIZE 8192
#define MAX_CLIENTS 100
volatile int running = 1;
int server_sock;
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
  int client_sock;
  struct sockaddr_in client_addr;
  char * server_dir;
}
client_args;
pthread_t client_threads[MAX_CLIENTS];
client_args thread_args[MAX_CLIENTS];
int thread_count = 0;
pthread_mutex_t thread_mutex = PTHREAD_MUTEX_INITIALIZER;
void error_handle(char * message);
void handle_signal(int sig);

void set_sock_timeout(int sock, int seconds);
char * get_base_filename(const char * filename);
ssize_t recv_data(int sock, void * buffer, size_t length);
ssize_t send_data(int sock,
  const void * buffer, size_t length);
void send_error_response(int client_sock, command_type cmd);
void build_chunk_filepath(char * filepath, size_t size,
  const char * dir,
    const char *
      client, time_t timestamp, int chunk_id, int total_chunks,
      const char * filename);
void handle_put_command(int client_sock,
  const message_header * header,
    const char *
      server_dir);
void handle_get_command(int client_sock,
  const message_header * header,
    const char *
      server_dir);
void handle_get_chunk_command(int client_sock,
  const message_header * header,
    const
      char * server_dir);
void handle_list_command(int client_sock,
  const char * dir_path,
    const char *
      filename);
void * client_handler(void * arg);
int main(int argc, char * argv[]) {
    if (argc != 3) {
      fprintf(stderr, "Usage: %s <dir> <port>\n", argv[0]);
      return 1;
    }
    char * server_dir = argv[1];
    int port = atoi(argv[2]);
    printf("Server dir: %s\n", server_dir);
    printf("Server listening on port: %d\n", port);
    struct stat dir_st = {
      0
    };
    if (stat(server_dir, & dir_st) == -1) {
      if (mkdir(server_dir, 0755) == -1) // will create dir if its not present
        error_handle("Directory creation error");
    }
    signal(SIGINT, handle_signal);
    server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0)
      error_handle("socket creation error");
    int opt = 1;
    if (setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, & opt, sizeof(opt)) < 0)
      error_handle("setsockopt error");
    struct sockaddr_in server_addr;
    memset( & server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(server_sock, (struct sockaddr * ) & server_addr, sizeof(server_addr)) <
      0)
      error_handle("bind error");
    if (listen(server_sock, 10) < 0)

      error_handle("listen error");
    printf("Server started on port %d.\n", port);
    pthread_mutex_init( & thread_mutex, NULL);
    while (running) {
      struct sockaddr_in client_addr;
      socklen_t client_addr_len = sizeof(client_addr);
      struct timeval timeout;
      timeout.tv_sec = 1; // to keep track of sigint signal
      timeout.tv_usec = 0;
      fd_set read_fds;
      FD_ZERO( & read_fds);
      FD_SET(server_sock, & read_fds);
      int select_result = select(server_sock + 1, & read_fds, NULL, NULL, &
        timeout);
      if (select_result < 0) {
        if (errno == EINTR)
          continue;
        perror("select error");
        break;
      }
      if (select_result == 0)
        continue;
      int client_sock = accept(server_sock, (struct sockaddr * ) & client_addr, &
        client_addr_len);
      if (client_sock < 0) {
        if (errno == EINTR || !running)
          continue;
        perror("Accept error");
        continue;
      }
      pthread_mutex_lock( & thread_mutex);
      if (thread_count >= MAX_CLIENTS) {
        pthread_mutex_unlock( & thread_mutex);
        fprintf(stderr, "Maximum client limit reached. Rejecting connection.\
          n ");
          close(client_sock);
          continue;
        }
        thread_args[thread_count].client_sock = client_sock;
        thread_args[thread_count].client_addr = client_addr;
        thread_args[thread_count].server_dir = server_dir;
        if (pthread_create( & client_threads[thread_count], NULL, client_handler, &
            thread_args[thread_count]) != 0)

        {
          pthread_mutex_unlock( & thread_mutex);
          perror("Thread creation failed");
          close(client_sock);
          continue;
        }
        pthread_detach(client_threads[thread_count]);
        thread_count++;
        pthread_mutex_unlock( & thread_mutex);
      }
      if (DEBUG == 1)
        printf("Server shutting down...\n");
      if (server_sock > 0) {
        close(server_sock);
      }
      pthread_mutex_destroy( & thread_mutex);
      printf("Server shutdown complete.\n");
      return 0;
    }
    void * client_handler(void * arg) {
      client_args * args = (client_args * ) arg;
      int client_sock = args -> client_sock;
      char * server_dir = args -> server_dir;
      if (DEBUG == 1)
        printf("New client connected\n");
      while (running) {
        message_header header;
        ssize_t header_size = sizeof(message_header);
        ssize_t received_data = recv_data(client_sock, & header, header_size); //
        will first receive a header always which will help in deciding which method is
        asked by client
        if (received_data != header_size) {
          if (DEBUG == 1)
            printf("Client disconnected\n");
          break;
        }
        switch (header.cmd) {
        case LIST_CMD:
          handle_list_command(client_sock, server_dir, header.filename);
          break;
        case PUT_CMD:
          handle_put_command(client_sock, & header, server_dir);
          break;

        case GET_CMD:
          handle_get_command(client_sock, & header, server_dir); // this will get
          the filname in header and
          return details from metadata as response
          break;
        case GET_CHUNK_CMD:
          handle_get_chunk_command(client_sock, & header, server_dir); // using
          the get command response it will now query exact details to get the appropriate
          file
          break;
        default:
          fprintf(stderr, "Unknown command type\n");
          break;
        }
      }
      close(client_sock);
      pthread_mutex_lock( & thread_mutex);
      for (int i = 0; i < thread_count; i++) {
        if (pthread_equal(pthread_self(), client_threads[i])) {
          if (i < thread_count - 1) {
            client_threads[i] = client_threads[thread_count - 1];
            thread_args[i] = thread_args[thread_count - 1];
          }
          thread_count--;
          break;
        }
      }
      pthread_mutex_unlock( & thread_mutex);
      pthread_exit(NULL);
    }
    void error_handle(char * message) {
      perror(message);
      exit(EXIT_FAILURE);
    }
    void handle_signal(int sig) {
      printf("\nReceived Ctrl+C signal. Shutting down server...\n");
      running = 0;
      if (server_sock > 0) {
        close(server_sock);
      }
    }
    void set_sock_timeout(int sock, int seconds) {

      struct timeval tv;
      tv.tv_sec = seconds;
      tv.tv_usec = 0;
      setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, & tv, sizeof(tv));
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, & tv, sizeof(tv));
    }
    char * get_base_filename(const char * filename) {
      if (filename != NULL || strcmp(filename, "") == 0) {
        char * basec, * bname;
        basec = strdup(filename);
        bname = basename(basec);
        return bname;
      }
      return NULL;
    }
    ssize_t recv_data(int sock, void * buffer, size_t length) {
      size_t total_received = 0;
      set_sock_timeout(sock, 10);
      while (total_received < length) {
        size_t to_read = (length - total_received < BUFFER_SIZE) ? length -
          total_received : BUFFER_SIZE;
        ssize_t bytes_received = recv(sock, (char * ) buffer + total_received,
          to_read, 0);
        if (bytes_received < 0) {
          if (errno == EINTR)
            continue;
          if (DEBUG == 1)
            printf("Receive error: %s\n", strerror(errno));
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
        size_t to_send = (length - total_sent < BUFFER_SIZE) ? length -
          total_sent : BUFFER_SIZE;
        ssize_t bytes_sent = send(sock, (const char * ) buffer + total_sent, to_send,

          0);
        if (bytes_sent < 0) {
          if (errno == EINTR)
            continue;
          if (DEBUG == 1)
            printf("Send error: %s\n", strerror(errno));
          return -1;
        } else if (bytes_sent == 0) {
          return total_sent > 0 ? total_sent : -1;
        }
        total_sent += bytes_sent;
      }
      return total_sent;
    }
    void send_error_response(int client_sock, command_type cmd) {
      message_header error_header;
      memset( & error_header, 0, sizeof(error_header));
      error_header.cmd = cmd;
      error_header.chunk_size = -1;
      strncpy(error_header.filename, "ERROR", sizeof(error_header.filename) - 1);
      send(client_sock, & error_header, sizeof(error_header), 0);
    }
    void build_chunk_filepath(char * filepath, size_t size,
      const char * dir,
        const char *
          client, time_t timestamp, int chunk_id, int total_chunks,
          const char * filename) {
      char * basec, * bname;
      basec = strdup(filename);
      bname = basename(basec);
      snprintf(filepath, size, "%s/%s_%ld_%d_%d_%s", dir, client, timestamp,
        chunk_id, total_chunks, bname);
    }
    void handle_put_command(int client_sock,
      const message_header * header,
        const char *
          server_dir) {
      char * chunk_data = malloc(header -> chunk_size);
      if (!chunk_data) {
        perror("Memory allocation error");
        send_error_response(client_sock, PUT_CMD);
        return;
      }
      if (recv_data(client_sock, chunk_data, header -> chunk_size) != header -
        >
        chunk_size) {
        if (DEBUG == 1)
          fprintf(stderr, "Incomplete chunk data received\n");
        free(chunk_data);
        send_error_response(client_sock, PUT_CMD);
        return;
      }

      char filepath[PATH_MAX];
      build_chunk_filepath(filepath, sizeof(filepath), server_dir, header -
        >
        client_name, header -> timestamp, header -> chunk_id, header -> total_chunks, header -
        >
        filename);
      if (DEBUG == 1)
        printf("Filepath: %s\n", filepath);
      FILE * fp = fopen(filepath, "wb");
      if (!fp) {
        perror("File creation error");
        free(chunk_data);
        send_error_response(client_sock, PUT_CMD);
        return;
      }
      size_t written = fwrite(chunk_data, 1, header -> chunk_size, fp);
      fclose(fp);
      free(chunk_data);
      if (written != header -> chunk_size) {
        if (DEBUG == 1)
          fprintf(stderr, "Complete chunk not written.\n");
        send_error_response(client_sock, PUT_CMD);
        return;
      }
      if (DEBUG == 1)
        ("Saved chunk %d/%d of file '%s'\n", header -> chunk_id, header -
          >
          total_chunks, header -> filename);
      message_header ack_header = {
        0
      };
      ack_header.cmd = PUT_CMD;
      ack_header.chunk_id = header -> chunk_id;
      ack_header.chunk_size = header -> chunk_size;
      send_data(client_sock, & ack_header, sizeof(ack_header));
    }
    void handle_get_command(int client_sock,
      const message_header * header,
        const char *
          server_dir) {
      DIR * dir = opendir(server_dir);
      if (!dir) {
        perror("Failed to open directory");
        send_error_response(client_sock, GET_CMD);
        return;
      }
      struct dirent * entry;
      int files_found = 0;
      char * bname = get_base_filename(header -> filename);
      while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry -> d_name, ".") == 0 || strcmp(entry -> d_name, "..") == 0)
          continue;

        if (strstr(entry -> d_name, bname) != NULL) {
          char client_name[100] = {
            0
          };
          time_t timestamp;
          int chunk_id, total_chunks;
          char extracted_filename[FILE_NAME_LIMIT] = {
            0
          };
          if (sscanf(entry -> d_name, "%[^_]_%ld_%d_%d_%s", client_name, &
              timestamp, & chunk_id, & total_chunks, extracted_filename) == 5) {
            message_header header_to_client = {
              0
            };
            strncpy(header_to_client.client_name, client_name,
              sizeof(header_to_client.client_name) - 1);
            strncpy(header_to_client.filename, extracted_filename,
              sizeof(header_to_client.filename) - 1);
            header_to_client.timestamp = timestamp;
            header_to_client.chunk_id = chunk_id;
            header_to_client.total_chunks = total_chunks;
            if (send_data(client_sock, & header_to_client,
                sizeof(header_to_client)) != sizeof(header_to_client)) {
              perror("send file info header error");
              closedir(dir);
              return;
            }
            files_found++;
          }
        }
      }
      closedir(dir);
      message_header header_done = {
        0
      };
      strncpy(header_done.filename, "DONE", sizeof(header_done.filename) - 1);
      send_data(client_sock, & header_done, sizeof(header_done));
      if (DEBUG == 1)
        printf("Found %d files matching %s filename\n", files_found, bname);
    }
    void handle_get_chunk_command(int client_sock,
      const message_header * header,
        const
          char * server_dir) {
      char filepath[PATH_MAX];
      build_chunk_filepath(filepath, sizeof(filepath), server_dir, header -
        >
        client_name, header -> timestamp, header -> chunk_id, header -> total_chunks, header -
        >
        filename);
      if (DEBUG == 1)
        printf("Retrieving chunk file: %s\n", filepath);
      struct stat fstat;
      if (stat(filepath, & fstat) != 0) {
        if (DEBUG == 1)
          printf("Chunk file not found\n");
        send_error_response(client_sock, GET_CHUNK_CMD);
        return;
      }

      FILE * chunk_file = fopen(filepath, "rb");
      if (!chunk_file) {
        perror("chunk file open error");
        send_error_response(client_sock, GET_CHUNK_CMD);
        return;
      }
      fseek(chunk_file, 0, SEEK_END);
      long chunk_size = ftell(chunk_file);
      rewind(chunk_file);
      message_header response = {
        0
      };
      response.cmd = GET_CHUNK_CMD;
      response.chunk_id = header -> chunk_id;
      response.total_chunks = header -> total_chunks;
      response.chunk_size = chunk_size;
      strncpy(response.filename, header -> filename, sizeof(response.filename) - 1);
      if (send_data(client_sock, & response, sizeof(response)) != sizeof(response)) {
        perror("send response header error");
        fclose(chunk_file);
        return;
      }
      if (DEBUG == 1)
        printf("Sending chunk %d of file %s\n", header -> chunk_id, header -
          >
          filename);
      char buffer[BUFFER_SIZE];
      size_t bytes_read;
      size_t total_sent = 0;
      while ((bytes_read = fread(buffer, 1, BUFFER_SIZE, chunk_file)) > 0) {
        if (send_data(client_sock, buffer, bytes_read) != bytes_read) {
          perror("send chunk data error");
          break;
        }
        total_sent += bytes_read;
      }
      if (DEBUG == 1)
        printf("sent chunk %d of file %s\n", header -> chunk_id, header -> filename);
      fclose(chunk_file);
    }
    void handle_list_command(int client_sock,
      const char * dir_path,
        const char *
          filename) {
      DIR * dir = opendir(dir_path);
      if (!dir) {
        perror("open dir error");
        send_error_response(client_sock, LIST_CMD);
        return;
      }

      struct dirent * entry;
      int files_found = 0;
      while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry -> d_name, ".") == 0 || strcmp(entry -> d_name, "..") == 0)
          continue;
        char client_name[100] = {
          0
        };
        time_t timestamp;
        int chunk_id, total_chunks;
        char extracted_filename[FILE_NAME_LIMIT] = {
          0
        };
        if (sscanf(entry -> d_name, "%[^_]_%ld_%d_%d_%s", client_name, & timestamp, &
            chunk_id, & total_chunks, extracted_filename) == 5) {
          if (filename && * filename && strcmp(extracted_filename, filename) != 0)
            // check if filename is exact match with query filename
            continue;
          message_header header_to_client = {
            0
          };
          strncpy(header_to_client.client_name, client_name,
            sizeof(header_to_client.client_name) - 1);
          strncpy(header_to_client.filename, extracted_filename,
            sizeof(header_to_client.filename) - 1);
          header_to_client.timestamp = timestamp;
          header_to_client.chunk_id = chunk_id;
          header_to_client.total_chunks = total_chunks;
          if (send_data(client_sock, & header_to_client, sizeof(header_to_client)) !=
            sizeof(header_to_client)) {
            perror("send file info header error");
            closedir(dir);
            return;
          }
          files_found++;
        }
      }
      closedir(dir);
      message_header header_done = {
        0
      };
      strncpy(header_done.filename, "DONE", sizeof(header_done.filename) - 1);
      send_data(client_sock, & header_done, sizeof(header_done));
      if (DEBUG == 1)
        printf("Completed list operation for %s. Found file count: %d\n",
          filename && * filename ? filename : "All", files_found);
    }
