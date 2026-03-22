#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <signal.h>

void handler(int sig){
    printf("\nSender interrupted\n");
    fflush(stdout);
    exit(0);
}

int main(int argc, char* argv[]){
    printf("Sender started, will connect within 10 sec...\n");
    init();

    int sockfd = k_socket(AF_INET, SOCK_KTP, 0);
    signal(SIGINT, handler);

    const char* myip = "127.0.0.1";
    const char* dstip = "127.0.0.1";
    int my_port = 5000;
    int dst_port = 6000;
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

    // open input file
    FILE *fp = fopen("input.txt", "r");
    if(fp == NULL){
        perror("fopen input.txt");
        return 1;
    }

    printf("Sending file...\n");
    char buf[MSG_SIZE];
    int bytes_read;
    int total_sent = 0;
    int chunk = 0;

    while((bytes_read = fread(buf, 1, MSG_SIZE, fp)) > 0){
        buf[bytes_read] = '\0'; 
        // retry if send_buf is full
        while(k_sendto(sockfd, buf, bytes_read, 0,
               (struct sockaddr*)&dstaddr, dstlen) == 0){
            usleep(100000);
        }
        total_sent += bytes_read;
        chunk++;
        printf("Sent chunk %d: %d bytes (total=%d)\n", chunk, bytes_read, total_sent);
    }

    fclose(fp);
    printf("File sent! Total bytes: %d\n", total_sent);

    // wait for last ACKs
    sleep(TIMEOUT * 4);
    k_close(sockfd);
    return 0;
}