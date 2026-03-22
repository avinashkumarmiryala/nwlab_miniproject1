#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <sys/select.h>
#include <errno.h>
#include <signal.h>

int shmid = -1;
sh_mem* sm = NULL;
int k_errno;
int first;

void sig_handler(int sig) {
    printf("\nCleaning up...\n");
    if (sm != NULL && sm != (void*)-1) {
        shmdt(sm);
        sm = NULL;
    }
    if (first && shmid != -1) {
        printf("Removing shared memory...\n");
        shmctl(shmid, IPC_RMID, NULL);
    }
    exit(0);
}


void init(){
    key_t key = ftok("documentation.txt", 1);

    first = 0;

    // Try to create exclusively
    shmid = shmget(key, sizeof(sh_mem), IPC_CREAT | IPC_EXCL | 0666);
    if(shmid == -1){
        // Already exists → just get it
        shmid = shmget(key, sizeof(sh_mem), 0666);
        if(shmid == -1){
            perror("shmget");
            exit(1);
        }
    } else {
        // This process created it
        first = 1;
    }

    sm = shmat(shmid, NULL, 0);
    if(sm == (void*)-1){
        perror("shmat");
        exit(1);
    }

    // Only FIRST process initializes
    if(first){
        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        pthread_mutex_init(&sm->lock, &attr);

        pthread_mutex_lock(&sm->lock);
        for(int i = 0; i < MAX_KTP_SOCK; i++){
            sm->sockets[i].is_free = 1;
        }
        sm->init = 1;   // mark initialized
        pthread_mutex_unlock(&sm->lock);
    }
}

int dropmsg(float p){
    float r = (float)rand() / RAND_MAX;
    if(r<p) return 1;
    return 0;
}

int k_socket(int domain, int type, int protocol){
    
    if(type!=SOCK_KTP) return -1;
    pthread_mutex_lock(&sm->lock);
    int idx = -1;
    for(int i=0;i<MAX_KTP_SOCK;i++){
        if (sm->sockets[i].is_free == 1){
            idx = i;            //free idx
            sm->sockets[i].is_free = 0;
            break;
        }
    }
    
    if(idx==-1){
        k_errno = ENOSPACE;
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }
    
    sm->sockets[idx].needs_udp_init = 1;
    sm->sockets[idx].udp_sockfd = -1;

    sm->sockets[idx].pid = getpid();

    sm->sockets[idx].send_buf.cnt = 0;
    sm->sockets[idx].send_buf.head = 0;
    sm->sockets[idx].send_buf.tail = 0;

    sm->sockets[idx].recv_buf.cnt = 0;
    sm->sockets[idx].recv_buf.head = 0;
    sm->sockets[idx].recv_buf.tail = 0;

    sm->sockets[idx].swnd.acked_wnd_size = 10;
    sm->sockets[idx].swnd.wnd_size = 5;
    sm->sockets[idx].swnd.start = 0;
    sm->sockets[idx].swnd.cnt = 0;
    sm->sockets[idx].swnd.next_seq_num = 1;

    sm->sockets[idx].rwnd.wnd_size = 10;
    sm->sockets[idx].rwnd.exptd_seq = 1;
    sm->sockets[idx].rwnd.last_ack = 0;
    sm->sockets[idx].rwnd.last_delivered = 0; 

    //init to check whether bound or not
    //if bound then the port value will be updated
    sm->sockets[idx].dest_addr.sin_port = 0;
    sm->sockets[idx].src_addr.sin_port = 0;

    sm->sockets[idx].nospace = 0;
    memset(sm->sockets[idx].rwnd.rcvd_seq, 255, BUF_SIZE);
    sm->sockets[idx].send_ack = 0; 

    pthread_mutex_unlock(&sm->lock);
    return idx;
}

int k_bind(int sockfd, const char *src_ip, int src_port, const char *dest_ip, int dest_port){
    pthread_mutex_lock(&sm->lock);
    
    // wait for Thread R to create UDP socket
    while(sm->sockets[sockfd].udp_sockfd == -1){
        pthread_mutex_unlock(&sm->lock);
        usleep(100000);
        pthread_mutex_lock(&sm->lock);
    }

    // just store the addresses — let Thread R do the actual bind
    sm->sockets[sockfd].src_addr.sin_addr.s_addr = inet_addr(src_ip);
    sm->sockets[sockfd].src_addr.sin_family = AF_INET;
    sm->sockets[sockfd].src_addr.sin_port = htons(src_port);

    sm->sockets[sockfd].dest_addr.sin_addr.s_addr = inet_addr(dest_ip);
    sm->sockets[sockfd].dest_addr.sin_family = AF_INET;
    sm->sockets[sockfd].dest_addr.sin_port = htons(dest_port);

    // signal Thread R to bind
    sm->sockets[sockfd].needs_bind = 1;

    // wait for Thread R to finish binding
    while(sm->sockets[sockfd].needs_bind == 1){
        pthread_mutex_unlock(&sm->lock);
        usleep(100000);
        pthread_mutex_lock(&sm->lock);
    }

    pthread_mutex_unlock(&sm->lock);
    return 0;
}

int k_sendto(int sockfd,const void *buf,size_t len,int flags,const struct sockaddr *dest_addr,socklen_t addrlen){
    pthread_mutex_lock(&sm->lock);
    if(dest_addr == NULL){
        k_errno = ENOTBOUND;
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }
    if(sockfd < 0 || sockfd >= MAX_KTP_SOCK){
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }
    if(sm->sockets[sockfd].dest_addr.sin_port == 0){
        //not yet bound
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }
    struct sockaddr_in *dest = (struct sockaddr_in*)(dest_addr);
    uint32_t daddr = sm->sockets[sockfd].dest_addr.sin_addr.s_addr;
    uint16_t dport = sm->sockets[sockfd].dest_addr.sin_port;
    int16_t dfamily = sm->sockets[sockfd].dest_addr.sin_family;
    //printf("k_sendto: sending to port %d\n\n", ntohs(dest->sin_port));
    if((daddr!=dest->sin_addr.s_addr) || (dport!=dest->sin_port) || (dfamily!=dest->sin_family)){
        k_errno = ENOTBOUND;
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }
    
    if(sm->sockets[sockfd].send_buf.cnt==BUF_SIZE) {
        //filled
        //0 bytes sent
        pthread_mutex_unlock(&sm->lock);
        return 0;
    }
    int t = sm->sockets[sockfd].send_buf.tail;
    sm->sockets[sockfd].send_buf.tail = (sm->sockets[sockfd].send_buf.tail + 1)%BUF_SIZE;

    if(len>MSG_SIZE) len = MSG_SIZE;
    sm->sockets[sockfd].send_buf.msg[t].len = len;
    memset(sm->sockets[sockfd].send_buf.msg[t].data, 0, MSG_SIZE); 
    memcpy(sm->sockets[sockfd].send_buf.msg[t].data, buf, len);

    //window size field is filled if type = ACK
    sm->sockets[sockfd].send_buf.msg[t].header.type = DATA;
    sm->sockets[sockfd].send_buf.msg[t].header.seq_num = sm->sockets[sockfd].swnd.next_seq_num;
    sm->sockets[sockfd].swnd.next_seq_num = (sm->sockets[sockfd].swnd.next_seq_num + 1)%256;

    sm->sockets[sockfd].send_buf.cnt+=1;

    pthread_mutex_unlock(&sm->lock);
    return len;
}

int k_recvfrom(int sockfd, void *buf, size_t len, int flags,
               struct sockaddr *src_addr, socklen_t *addrlen){
    pthread_mutex_lock(&sm->lock);

    if(sockfd < 0 || sockfd >= MAX_KTP_SOCK){
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }

    if(sm->sockets[sockfd].recv_buf.cnt == 0){
        k_errno = ENOMESSAGE;
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }

    // find the next in-order packet to deliver
    uint8_t want = (sm->sockets[sockfd].rwnd.last_delivered + 1) % SEQ_NUM_MOD;
    // on first delivery, want = 1 (since last_delivered starts at 0)

    int found = -1;
    for(int k = 0; k < sm->sockets[sockfd].recv_buf.cnt; k++){
        int idx = (sm->sockets[sockfd].recv_buf.head + k) % BUF_SIZE;
        if(sm->sockets[sockfd].recv_buf.msg[idx].header.seq_num == want){
            found = idx;
            break;
        }
    }

    if(found == -1){
        // next in-order packet not yet arrived
        k_errno = ENOMESSAGE;
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }

    // copy data out
    size_t copy_len = sm->sockets[sockfd].recv_buf.msg[found].len;
    if(copy_len > len) copy_len = len;
    memcpy(buf, sm->sockets[sockfd].recv_buf.msg[found].data, copy_len);
    sm->sockets[sockfd].rwnd.last_delivered = want;

    // remove from recv_buf by shifting head
    // (swap found slot with head slot, then advance head)
    int head = sm->sockets[sockfd].recv_buf.head;
    if(found != head){
        // swap so we can just advance head
        ktp_packet tmp = sm->sockets[sockfd].recv_buf.msg[head];
        sm->sockets[sockfd].recv_buf.msg[head] = sm->sockets[sockfd].recv_buf.msg[found];
        sm->sockets[sockfd].recv_buf.msg[found] = tmp;
    }
    sm->sockets[sockfd].recv_buf.head = (head + 1) % BUF_SIZE;
    sm->sockets[sockfd].recv_buf.cnt--;
    sm->sockets[sockfd].rwnd.wnd_size = BUF_SIZE - sm->sockets[sockfd].recv_buf.cnt;

    if(sm->sockets[sockfd].nospace == 1){
        sm->sockets[sockfd].nospace = 0;
        sm->sockets[sockfd].send_ack = 1;
    }

    if(src_addr != NULL){
        struct sockaddr_in *src = (struct sockaddr_in*)src_addr;
        src->sin_addr.s_addr = sm->sockets[sockfd].dest_addr.sin_addr.s_addr;
        src->sin_family      = sm->sockets[sockfd].dest_addr.sin_family;
        src->sin_port        = sm->sockets[sockfd].dest_addr.sin_port;
    }
    if(addrlen != NULL) *addrlen = sizeof(struct sockaddr_in);

    pthread_mutex_unlock(&sm->lock);
    return copy_len;
}


int k_close(int ksockfd){
    pthread_mutex_lock(&sm->lock);
    int udpsock = sm->sockets[ksockfd].udp_sockfd;
    int c = close(udpsock);
    
    sm->sockets[ksockfd].is_free = 1;
    pthread_mutex_unlock(&sm->lock);
    return c;
}