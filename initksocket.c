#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <sys/select.h>

int min(int a, int b) {
    if(a>b) return b;
    return a;
}

void* R(void *arg){
    //HARSHA
    
    while(1){
        // Build fd_set from all active sockets
        fd_set readfds;
        FD_ZERO(&readfds);
        int max_fd = 0;

        pthread_mutex_lock(&sm->lock);
        for(int i = 0; i < MAX_KTP_SOCK; i++){
            if(!sm->sockets[i].is_free && 
               sm->sockets[i].dest_addr.sin_port != 0){
                FD_SET(sm->sockets[i].udp_sockfd, &readfds);
                if(sm->sockets[i].udp_sockfd > max_fd)
                    max_fd = sm->sockets[i].udp_sockfd;
            }
        }
        pthread_mutex_unlock(&sm->lock);

        // select with timeout
        struct timeval timeout;
        timeout.tv_sec = TIMEOUT;
        timeout.tv_usec = 0;

        int ready = select(max_fd+1, &readfds, NULL, NULL, &timeout);

        //  Timeout 
        if(ready == 0){
         pthread_mutex_lock(&sm->lock);
         for(int i = 0; i < MAX_KTP_SOCK; i++){
            if(sm->sockets[i].is_free) continue;
            if(sm->sockets[i].dest_addr.sin_port == 0) continue;

             if(sm->sockets[i].send_ack == 1){
            sm->sockets[i].send_ack = 0;
            sm->sockets[i].rwnd.wnd_size = BUF_SIZE - sm->sockets[i].recv_buf.cnt;

            ktp_packet ack_pkt;
            memset(&ack_pkt, 0, sizeof(ack_pkt));
            ack_pkt.header.type = ACK;
            ack_pkt.header.seq_num = sm->sockets[i].rwnd.last_ack;
            ack_pkt.header.rwnd = sm->sockets[i].rwnd.wnd_size;

            struct sockaddr_in dst = sm->sockets[i].dest_addr;
            socklen_t dstlen = sizeof(dst);
            sendto(sm->sockets[i].udp_sockfd, &ack_pkt, sizeof(ack_pkt),
                   0, (struct sockaddr*)&dst, dstlen);
        }
    }
    pthread_mutex_unlock(&sm->lock);
}

        // Packet arrived
        if(ready > 0){
            pthread_mutex_lock(&sm->lock);
            for(int i = 0; i < MAX_KTP_SOCK; i++){
                if(sm->sockets[i].is_free) continue;
                if(sm->sockets[i].dest_addr.sin_port == 0) continue;
                if(!FD_ISSET(sm->sockets[i].udp_sockfd, &readfds)) continue;

                // receive the packet
                ktp_packet pkt;
                struct sockaddr_in src;
                socklen_t srclen = sizeof(src);
                recvfrom(sm->sockets[i].udp_sockfd, &pkt, sizeof(pkt),
                         0, (struct sockaddr*)&src, &srclen);

                // simulate drop
                if(dropmsg(DROP_PROB)) continue;

               
                if(pkt.header.type == DATA){
                    uint8_t seq = pkt.header.seq_num;

                    // check duplicate
                    int duplicate = 0;
                    for(int k = 0; k < BUF_SIZE; k++){
                        if(sm->sockets[i].rwnd.rcvd_seq[k] == seq){
                            duplicate = 1;
                            break;
                        }
                    }
                    if(duplicate) continue;

                    // check if recv_buf is full
                    if(sm->sockets[i].recv_buf.cnt == BUF_SIZE){
                        sm->sockets[i].nospace = 1;
                        continue;
                    }

                    // store in recv_buf
                    int tail = sm->sockets[i].recv_buf.tail;
                    sm->sockets[i].recv_buf.msg[tail] = pkt;
                    sm->sockets[i].recv_buf.tail = (tail + 1) % BUF_SIZE;
                    sm->sockets[i].recv_buf.cnt++;

                    // mark as received
                    sm->sockets[i].rwnd.rcvd_seq[tail] = seq;

                    // update rwnd size
                    sm->sockets[i].rwnd.wnd_size = BUF_SIZE - sm->sockets[i].recv_buf.cnt;

                    // check if in order
                    if(seq == sm->sockets[i].rwnd.exptd_seq){
                        // advance exptd_seq
                        sm->sockets[i].rwnd.last_ack = seq;
                        sm->sockets[i].rwnd.exptd_seq = (seq + 1) % SEQ_NUM_MOD;

                        // send ACK
                        ktp_packet ack_pkt;
                        memset(&ack_pkt, 0, sizeof(ack_pkt));
                        ack_pkt.header.type = ACK;
                        ack_pkt.header.seq_num = sm->sockets[i].rwnd.last_ack;
                        ack_pkt.header.rwnd = sm->sockets[i].rwnd.wnd_size;

                        struct sockaddr_in dst = sm->sockets[i].dest_addr;
                        socklen_t dstlen = sizeof(dst);
                        sendto(sm->sockets[i].udp_sockfd, &ack_pkt, sizeof(ack_pkt),
                               0, (struct sockaddr*)&dst, dstlen);
                    }
                    // out of order — store but don't ACK
                }

              
                else if(pkt.header.type == ACK){
                    uint8_t ack_no = pkt.header.seq_num;
                    uint8_t new_rwnd = pkt.header.rwnd;
                    sm->sockets[i].swnd.wnd_size = new_rwnd;

                    // slide window 
                    while(sm->sockets[i].swnd.cnt > 0){
                        int idx = sm->sockets[i].swnd.start % BUF_SIZE;
                        uint8_t seq = sm->sockets[i].send_buf.msg[idx].header.seq_num;

                        // check if this message is ACKed
                        int acked = 0;
                        // simple check without wrap around
                        if(seq == ack_no) acked = 1;
                        // check all seq nums between start and ack_no
                        uint8_t s = sm->sockets[i].send_buf.msg[
                            sm->sockets[i].swnd.start % BUF_SIZE
                        ].header.seq_num;

                        if(s == ack_no){
                            // remove this message
                            sm->sockets[i].swnd.start = (sm->sockets[i].swnd.start + 1) % BUF_SIZE;
                            sm->sockets[i].send_buf.head = (sm->sockets[i].send_buf.head + 1) % BUF_SIZE;
                            sm->sockets[i].send_buf.cnt--;
                            sm->sockets[i].swnd.cnt--;
                            break;
                        } else {
                            // remove messages before ack_no
                            sm->sockets[i].swnd.start = (sm->sockets[i].swnd.start + 1) % BUF_SIZE;
                            sm->sockets[i].send_buf.head = (sm->sockets[i].send_buf.head + 1) % BUF_SIZE;
                            sm->sockets[i].send_buf.cnt--;
                            sm->sockets[i].swnd.cnt--;
                        }
                    }
                }
            }
            pthread_mutex_unlock(&sm->lock);
        }
    }
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

            int eff_wnd = min(sm->sockets[i].swnd.wnd_size,sm->sockets[i].rwnd.wnd_size);

            while ((sm->sockets[i].swnd.cnt < eff_wnd) &&
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