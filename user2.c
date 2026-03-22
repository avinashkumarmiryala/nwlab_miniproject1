#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <signal.h>

void handler(int sig){
    printf("\nReceiver interrupted\n");
    fflush(stdout);
    exit(0);
}

int main(int argc, char* argv[]){
    printf("Receiver started, waiting for file...\n");
    init();
    signal(SIGINT, handler);

    int sockfd = k_socket(AF_INET, SOCK_KTP, 0);

    const char* myip = "127.0.0.1";
    const char* dstip = "127.0.0.1";
    int my_port = 6000;
    int dst_port = 5000;
    if(argc > 1) my_port = atoi(argv[1]);
    if(argc > 2) dst_port = atoi(argv[2]);

    struct sockaddr_in dstaddr;
    dstaddr.sin_addr.s_addr = inet_addr(dstip);
    dstaddr.sin_family = AF_INET;
    dstaddr.sin_port = htons(dst_port);
    socklen_t dstlen = sizeof(dstaddr);

    int b = k_bind(sockfd, myip, my_port, dstip, dst_port);
    if(b == -1){
        printf("Bind Error!\n");
        return 0;
    }

    // open output file
    FILE *fp = fopen("output.txt", "w");
    if(fp == NULL){
        perror("fopen output.txt");
        return 1;
    }

    printf("Waiting for data...\n");
    char buf[MSG_SIZE];
    int total_bytes = 0;
    int chunk = 0;

    while(1){
        int n = k_recvfrom(sockfd, buf, MSG_SIZE, 0,
                           (struct sockaddr*)&dstaddr, &dstlen);
        if(n <= 0){
            //printf("k_recvfrom returned %d, errno=%d\n", n, k_errno); 
            usleep(100000);
            continue;
        }
        fwrite(buf, 1, n, fp);
        fflush(fp);  // flush after each chunk
        total_bytes += n;
        chunk++;
        printf("Received chunk %d: %d bytes (total=%d)\n", chunk, n, total_bytes);
    }

    fclose(fp);
    k_close(sockfd);
    return 0;
}