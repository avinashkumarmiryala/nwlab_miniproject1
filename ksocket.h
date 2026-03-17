#ifndef _KSOCKET_H
#define _KSOCKET_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <netinet/in.h>
#include <unistd.h>
#include <time.h>


#define SOCK_KTP 5

#define MAX_KTP_SOCK 10
#define BUF_SIZE 10
#define MSG_SIZE 512

#define SEQ_NUM_MOD 256

#define TIMEOUT 5        // T seconds
#define DROP_PROB 0.05   // p value (change during testing)

#define ENOSPACE 1001
#define ENOTBOUND 1002
#define ENOMESSAGE 1003

extern int k_errno;


#define DATA 1
#define ACK 2


typedef struct
{
    uint8_t type;       // data or ack
    uint8_t seq_num;    
    uint8_t rwnd;       
} ktp_header;



typedef struct
{
    ktp_header header;
    char data[MSG_SIZE];
    uint16_t len;
} ktp_packet;



typedef struct
{
    int wnd_size;
    uint8_t seq_nums[BUF_SIZE];
    time_t send_time[BUF_SIZE];
    uint8_t next_seq_num;
    int start;
    int cnt;
} swnd_t;


typedef struct
{
    int wnd_size;
    uint8_t exptd_seq;
    uint8_t last_ack;
    uint8_t rcvd_seq[BUF_SIZE];
} rwnd_t;



typedef struct
{
    ktp_packet msg[BUF_SIZE];
    int head;
    int tail;
    int cnt;
} s_buf_t;



typedef struct
{
    ktp_packet msg[BUF_SIZE];
    int head;
    int tail;
    int cnt;
} r_buf_t;

//ktp socket entry in shared memory

typedef struct
{
    int is_free;
    pid_t pid;
    int udp_sockfd;

    struct sockaddr_in src_addr;
    struct sockaddr_in dest_addr;

    s_buf_t send_buf;
    r_buf_t recv_buf;

    swnd_t swnd;
    rwnd_t rwnd;

    int nospace;
    int send_ack;
} ktp_sock;

//shared memory structure

typedef struct
{
    ktp_sock sockets[MAX_KTP_SOCK];
    pthread_mutex_t lock;
} sh_mem;


extern sh_mem* sm;

void init();

int k_socket(int domain, int type, int protocol);

int k_bind(int sockfd,
           const char *src_ip,
           int src_port,
           const char *dest_ip,
           int dest_port);

int k_sendto(int sockfd,
             const void *buf,
             size_t len,
             int flags,
             const struct sockaddr *dest_addr,
             socklen_t addrlen);

int k_recvfrom(int sockfd,
               void *buf,
               size_t len,
               int flags,
               struct sockaddr *src_addr,
               socklen_t *addrlen);

int k_close(int sockfd);

int dropmsg(float p);

#endif