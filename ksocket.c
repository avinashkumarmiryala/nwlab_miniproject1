#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <sys/select.h>


int shmid;

//init the mem
void init(){
    key_t key = ftok("documentation.txt",1);

    shmid = shmget(key, sizeof(sh_mem), IPC_CREAT | 0666);
    if(shmid < 0){
        perror("shmget");
        exit(1);
    }

    sm = shmat(shmid, NULL, 0);
    if(sm == (void*)-1){
        perror("shmat");
        exit(1);
    }

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);

    pthread_mutex_init(&sm->lock, &attr);

    pthread_mutex_lock(&sm->lock);
    for(int i=0;i<MAX_KTP_SOCK;i++){
        sm->sockets[i].is_free = 1;
    }
    pthread_mutex_unlock(&sm->lock);
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
    
    int sfd = socket(AF_INET,SOCK_DGRAM,0);
    if (sfd < 0){
        sm->sockets[idx].is_free = 1;
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }
    sm->sockets[idx].udp_sockfd = sfd;

    sm->sockets[idx].pid = getpid();

    sm->sockets[idx].send_buf.cnt = 0;
    sm->sockets[idx].send_buf.head = 0;
    sm->sockets[idx].send_buf.tail = 0;

    sm->sockets[idx].recv_buf.cnt = 0;
    sm->sockets[idx].recv_buf.head = 0;
    sm->sockets[idx].recv_buf.tail = 0;

    sm->sockets[idx].swnd.wnd_size = 10;
    sm->sockets[idx].swnd.start = 0;
    sm->sockets[idx].swnd.cnt = 0;
    sm->sockets[idx].swnd.next_seq_num = 1;

    sm->sockets[idx].rwnd.wnd_size = 10;
    sm->sockets[idx].rwnd.exptd_seq = 1;
    sm->sockets[idx].rwnd.last_ack = 0;

    //init to check whether bound or not
    //if bound then the port value will be updated
    sm->sockets[idx].dest_addr.sin_port = 0;
    sm->sockets[idx].src_addr.sin_port = 0;

    sm->sockets[idx].nospace = 0;

    pthread_mutex_unlock(&sm->lock);
    return idx;
}

int k_bind(int sockfd, const char *src_ip, int src_port, const char *dest_ip, int dest_port){
    pthread_mutex_lock(&sm->lock);
    int udpsock = sm->sockets[sockfd].udp_sockfd;

    sm->sockets[sockfd].src_addr.sin_addr.s_addr = inet_addr(src_ip);
    sm->sockets[sockfd].src_addr.sin_family = AF_INET;
    sm->sockets[sockfd].src_addr.sin_port = htons(src_port);

    struct sockaddr_in src = sm->sockets[sockfd].src_addr;
    socklen_t srclen = sizeof(src);
    int b = bind(udpsock,(struct sockaddr*)&(src),srclen);
    if(b == -1) {
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }

    sm->sockets[sockfd].dest_addr.sin_addr.s_addr = inet_addr(dest_ip);
    sm->sockets[sockfd].dest_addr.sin_family = AF_INET;
    sm->sockets[sockfd].dest_addr.sin_port = htons(dest_port);
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
    memcpy(sm->sockets[sockfd].send_buf.msg[t].data, buf, len); //check once more

    //window size field is filled if type = ACK
    sm->sockets[sockfd].send_buf.msg[t].header.type = DATA;
    sm->sockets[sockfd].send_buf.msg[t].header.seq_num = sm->sockets[sockfd].swnd.next_seq_num;
    sm->sockets[sockfd].swnd.next_seq_num = (sm->sockets[sockfd].swnd.next_seq_num + 1)%256;

    sm->sockets[sockfd].send_buf.cnt+=1;

    pthread_mutex_unlock(&sm->lock);
    return len;
}

int k_recvfrom(int sockfd,void *buf,size_t len,int flags,struct sockaddr *src_addr,socklen_t *addrlen){

    pthread_mutex_lock(&sm->lock);
    if(sockfd < 0 || sockfd >= MAX_KTP_SOCK){
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }
    
    if(sm->sockets[sockfd].recv_buf.cnt==0) {
        k_errno = ENOMESSAGE;
        pthread_mutex_unlock(&sm->lock);
        return -1;
    }

    int h = sm->sockets[sockfd].recv_buf.head;
    sm->sockets[sockfd].recv_buf.head = (sm->sockets[sockfd].recv_buf.head + 1)%BUF_SIZE;

    size_t copy_len = sm->sockets[sockfd].recv_buf.msg[h].len;

    if(copy_len > len)
        copy_len = len;

    memcpy(buf,sm->sockets[sockfd].recv_buf.msg[h].data,copy_len);    

    if(sm->sockets[sockfd].rwnd.wnd_size < BUF_SIZE)  sm->sockets[sockfd].rwnd.wnd_size++;

    sm->sockets[sockfd].recv_buf.cnt-=1;

    if(src_addr != NULL){
        struct sockaddr_in *src = (struct sockaddr_in*)(src_addr);
        uint32_t srcaddr = sm->sockets[sockfd].dest_addr.sin_addr.s_addr;
        uint16_t srcport = sm->sockets[sockfd].dest_addr.sin_port;
        int16_t srcfamily = sm->sockets[sockfd].dest_addr.sin_family;

        src->sin_addr.s_addr = srcaddr;
        src->sin_family = srcfamily;
        src->sin_port = srcport;
    }

    if(addrlen != NULL){
        *addrlen = sizeof(struct sockaddr_in);
    }

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