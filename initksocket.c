#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <sys/select.h>


void* R(void *arg){
    //HARSHA

}
void* S(void *arg){
    //AVINASH
    while(1){
        int t = (TIMEOUT * 1e6)/2;
        usleep(t);
        pthread_mutex_lock(&sm->lock);
        for(int i=0;i<MAX_KTP_SOCK;i++){
            if(sm->sockets[i].is_free || sm->sockets[i].dest_addr.sin_port == 0) continue;

            if(sm->sockets[i].swnd.cnt>0){
                int idx = sm->sockets[i].swnd.start;

                if(time(NULL)-sm->sockets[i].swnd.send_time[idx] > TIMEOUT){
                    int bufstart = sm->sockets[i].swnd.start;

                    //resend all msg in the window
                    for(int k=0;k<sm->sockets[i].swnd.cnt;k++){
                        int j = (bufstart + k) % BUF_SIZE;
                        ktp_packet pkt = sm->sockets[i].send_buf.msg[j];
                        int l = sizeof(pkt);

                        int udpsock = sm->sockets[i].udp_sockfd;
                        struct sockaddr_in dst = sm->sockets[i].dest_addr;
                        socklen_t dstlen = sizeof(struct sockaddr_in);

                        //send over UDP
                        sendto(udpsock,&pkt,l,0,(struct sockaddr*)&dst,dstlen);
                        //reset timer
                        sm->sockets[i].swnd.send_time[j] = time(NULL);
                    }
                }
            }

            while ((sm->sockets[i].swnd.cnt < sm->sockets[i].swnd.wnd_size) && 
            (sm->sockets[i].send_buf.cnt > sm->sockets[i].swnd.cnt)){
                int idx = (sm->sockets[i].send_buf.head + sm->sockets[i].swnd.cnt) % BUF_SIZE;

                ktp_packet pkt = sm->sockets[i].send_buf.msg[idx];
                int l = sizeof(pkt);

                int udpsock = sm->sockets[i].udp_sockfd;
                struct sockaddr_in dst = sm->sockets[i].dest_addr;
                socklen_t dstlen = sizeof(struct sockaddr_in);
                sendto(udpsock,&pkt,l,0,(struct sockaddr*)&dst,dstlen);

                int win = (sm->sockets[i].swnd.start + sm->sockets[i].swnd.cnt) % BUF_SIZE;
                //one packet added to the window
                sm->sockets[i].swnd.cnt++;

                //update its send_time
                sm->sockets[i].swnd.send_time[win] = time(NULL);
            }
            
        }
        pthread_mutex_unlock(&sm->lock);
    }
}

int main()
{
    pthread_t tidS, tidR;
    init();

    if(pthread_create(&tidS, NULL, S, NULL) != 0){
        perror("pthread_create S");
        exit(1);
    }

    if(pthread_create(&tidR, NULL, R, NULL) != 0){
        perror("pthread_create R");
        exit(1);
    }

    pthread_join(tidS, NULL);
    pthread_join(tidR, NULL);
    return 0;
}