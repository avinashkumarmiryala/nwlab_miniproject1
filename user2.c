#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#define N 256
#define MAX_CLIENTS 5

void handler(int sig) {
    printf("\nReceiver interrupted\n");
    fflush(stdout);
    exit(0);
}

typedef struct {
    int sockfd;
    int client_port;   // which client this socket talks to
    int server_port;   // which server port this socket is bound to
} client_ctx;

void* handle_client(void* arg) {
    signal(SIGINT, handler);
    client_ctx* ctx = (client_ctx*)arg;
    int sockfd = ctx->sockfd;
    int client_port = ctx->client_port;
    int server_port = ctx->server_port;
    free(ctx);

    // dest_addr for sendto — matches exactly what k_bind registered
    struct sockaddr_in dstaddr;
    dstaddr.sin_family = AF_INET;
    dstaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    dstaddr.sin_port = htons(client_port);
    socklen_t dstlen = sizeof(dstaddr);

    printf("[Thread port %d] Ready, waiting for client on port %d\n",
           server_port, client_port);

    while (1) {
        char rbuf[N];
        // src_addr param doesn't matter much here since
        // reply dest is fixed at bind time anyway
        struct sockaddr_in src;
        socklen_t srclen = sizeof(src);

        // block until a message arrives
        int rbytes;
        while (1) {
            rbytes = k_recvfrom(sockfd, rbuf, sizeof(rbuf), 0,
                                (struct sockaddr*)&src, &srclen);
            if (rbytes > 0) break;
            if (rbytes == 0) {
                printf("[Thread port %d] Connection closed\n", server_port);
                return NULL;
            }
            usleep(50000);
        }

        printf("[Thread port %d] Got from client %d: %s\n",
               server_port, client_port, rbuf);

        printf("[Thread port %d] Enter reply: ", server_port);
        char sbuf[N];
        fgets(sbuf, N, stdin);
        sbuf[strcspn(sbuf, "\n")] = '\0';

        k_sendto(sockfd, sbuf, strlen(sbuf) + 1, 0,
                 (struct sockaddr*)&dstaddr, dstlen);
        printf("\n\n");
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    printf("Server started, waiting for clients...\n");
    init();
    signal(SIGINT, handler);

    int base_server_port = 6000; 
    int base_client_port = 5000; 
    int num_clients = MAX_CLIENTS;

    pthread_t tids[MAX_CLIENTS];

    for (int i = 0; i < num_clients; i++) {
        int sockfd = k_socket(AF_INET, SOCK_KTP, 0);
        if (sockfd < 0) {
            printf("k_socket failed for client %d\n", i);
            exit(1);
        }

        int sp = base_server_port + i;
        int cp = base_client_port + i;

        int b = k_bind(sockfd, "127.0.0.1", sp, "127.0.0.1", cp);
        if (b < 0) {
            printf("k_bind failed for client %d\n", i);
            exit(1);
        }

        client_ctx* ctx = malloc(sizeof(client_ctx));
        ctx->sockfd = sockfd;
        ctx->server_port = sp;
        ctx->client_port = cp;

        pthread_create(&tids[i], NULL, handle_client, ctx);
        printf("Spawned handler for client %d (server:%d <-> client:%d)\n",
               i, sp, cp);
    }


    for (int i = 0; i < num_clients; i++)
        pthread_join(tids[i], NULL);

    return 0;
}