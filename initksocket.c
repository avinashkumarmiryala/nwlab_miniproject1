#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <sys/select.h>
#include <signal.h>
const char *strtype[] = {"MSG", "DATA", "ACK"};
int min(int a, int b) {
    if(a>b) return b;
    return a;
}

void* R(void *arg){
    //HARSHA
    printf("R thread running\n");
    
    while(1){
        pthread_mutex_lock(&sm->lock);
        for(int i = 0; i < MAX_KTP_SOCK; i++){
            if(!sm->sockets[i].is_free && sm->sockets[i].needs_udp_init == 1){
                int sfd = socket(AF_INET, SOCK_DGRAM, 0);
                sm->sockets[i].udp_sockfd = sfd;
                sm->sockets[i].needs_udp_init = 0;
                printf("Thread R: created UDP socket %d for KTP socket %d\n", sfd, i);
            }
            if(!sm->sockets[i].is_free && sm->sockets[i].needs_bind == 1){
                struct sockaddr_in src = sm->sockets[i].src_addr;
                int b = bind(sm->sockets[i].udp_sockfd,
                            (struct sockaddr*)&src, sizeof(src));
                if(b == 0){
                    printf("Thread R: socket %d bound to port %d, watching for packets from port %d\n",i,
                        ntohs(sm->sockets[i].src_addr.sin_port), ntohs(sm->sockets[i].dest_addr.sin_port));
                } else {
                    perror("Thread R bind");
                }
                sm->sockets[i].needs_bind = 0;
            }
        }
        pthread_mutex_unlock(&sm->lock);
        fd_set readfds;
        FD_ZERO(&readfds);
        int max_fd = 0;

        pthread_mutex_lock(&sm->lock);
        for(int i = 0; i < MAX_KTP_SOCK; i++){
            if((!sm->sockets[i].is_free && sm->sockets[i].src_addr.sin_port) != 0 ){
                FD_SET(sm->sockets[i].udp_sockfd, &readfds);
                if(sm->sockets[i].udp_sockfd > max_fd)
                    max_fd = sm->sockets[i].udp_sockfd;
            }
        }
        pthread_mutex_unlock(&sm->lock);
        if(max_fd == 0){
            usleep(500000);  // sleep 0.1 seconds if no active sockets
            continue;
        }
        
        struct timeval timeout;
        timeout.tv_sec = TIMEOUT;
        timeout.tv_usec = 0;

        int ready = select(max_fd+1, &readfds, NULL, NULL, &timeout);
        if(ready < 0){
            perror("select error");
            continue;
        }
        //  Timeout 
        if(ready == 0){
            pthread_mutex_lock(&sm->lock);
            printf("Select TIMED OUT\n");
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
                printf("Thread R: received packet on socket %d\n", i);
                printf("Thread R: type = %s, seq = %d\n", strtype[pkt.header.type], pkt.header.seq_num);
                // simulate drop
                if(dropmsg(DROP_PROB)){
                    printf("Thread R: packet dropped!\n");
                    continue;
                }
               
                if(pkt.header.type == DATA){
                    uint8_t seq = pkt.header.seq_num;

                    // check duplicate
                int duplicate = 0;

                // check 1: if seq is behind exptd_seq — already processed
                // check 1: already processed — behind exptd OR behind last_delivered
                uint8_t expected = sm->sockets[i].rwnd.exptd_seq;
                uint8_t last_del = sm->sockets[i].rwnd.last_delivered;

                uint8_t diff1 = (uint8_t)(seq - expected);
                uint8_t diff2 = (uint8_t)(seq - last_del);

                if(diff1 > 128 || (last_del != 0 && diff2 > 128)){
                    duplicate = 1;
                }

                // check 2: if seq is already in recv_buf
                if(!duplicate){
                    for(int k = 0; k < sm->sockets[i].recv_buf.cnt; k++){
                        int idx = (sm->sockets[i].recv_buf.head + k) % BUF_SIZE;
                        if(sm->sockets[i].recv_buf.msg[idx].header.seq_num == seq){
                            duplicate = 1;
                            break;
                        }
                    }
                }

                if(duplicate){
                    ktp_packet ack_pkt;
                    memset(&ack_pkt, 0, sizeof(ack_pkt));
                    ack_pkt.header.type = ACK;
                    ack_pkt.header.seq_num = sm->sockets[i].rwnd.last_ack;
                    ack_pkt.header.rwnd = sm->sockets[i].rwnd.wnd_size;
                    struct sockaddr_in dst = sm->sockets[i].dest_addr;
                    socklen_t dstlen = sizeof(dst);
                    sendto(sm->sockets[i].udp_sockfd, &ack_pkt, sizeof(ack_pkt),
                        0, (struct sockaddr*)&dst, dstlen);
                    continue;
                }
                    // check if recv_buf is full
                    if(sm->sockets[i].recv_buf.cnt == BUF_SIZE){
                        sm->sockets[i].nospace = 1;
                        continue;
                    }

                    // store in recv_buf
                    printf("STORING seq=%d exptd=%d recv_cnt=%d\n", 
                    seq, 
                    sm->sockets[i].rwnd.exptd_seq,
                    sm->sockets[i].recv_buf.cnt);
                    int tail = sm->sockets[i].recv_buf.tail;
                    sm->sockets[i].recv_buf.msg[tail] = pkt;
                    sm->sockets[i].recv_buf.tail = (tail + 1) % BUF_SIZE;
                    sm->sockets[i].recv_buf.cnt++;
                    printf("Thread R: stored in recv_buf, cnt = %d\n\n", sm->sockets[i].recv_buf.cnt);
                    // mark as received
                    sm->sockets[i].rwnd.rcvd_seq[tail] = seq;

                    // update rwnd size
                    printf("Thread R: Reciever wnd size before update: %d\n", sm->sockets[i].rwnd.wnd_size);
                    sm->sockets[i].rwnd.wnd_size = BUF_SIZE - sm->sockets[i].recv_buf.cnt;
                    printf("Thread R: Reciever wnd size after update: %d\n\n", sm->sockets[i].rwnd.wnd_size);

                    // check if in order
                    if(seq == sm->sockets[i].rwnd.exptd_seq){
                        // advance exptd_seq
                        sm->sockets[i].rwnd.last_ack = seq;
                        sm->sockets[i].rwnd.exptd_seq = (seq + 1) % SEQ_NUM_MOD;
                        printf("After advancing: exptd=%d recv_cnt=%d\n",
                        sm->sockets[i].rwnd.exptd_seq,
                        sm->sockets[i].recv_buf.cnt);

                        int keep_going = 1;
                        while(keep_going){
                            keep_going = 0;
                            for(int k = 0; k < sm->sockets[i].recv_buf.cnt; k++){
                                int idx = (sm->sockets[i].recv_buf.head + k) % BUF_SIZE;
                                printf("Checking buf[%d] seq=%d vs exptd=%d\n",
                                    idx,
                                    sm->sockets[i].recv_buf.msg[idx].header.seq_num,
                                    sm->sockets[i].rwnd.exptd_seq);
                                if(sm->sockets[i].recv_buf.msg[idx].header.seq_num ==
                                sm->sockets[i].rwnd.exptd_seq){
                                    sm->sockets[i].rwnd.last_ack = sm->sockets[i].rwnd.exptd_seq;
                                    sm->sockets[i].rwnd.exptd_seq = (sm->sockets[i].rwnd.exptd_seq + 1) % SEQ_NUM_MOD;
                                    keep_going = 1;
                                    break;
                                }
                            }
                        }
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
                        printf("Thread R: sent ACK for seq %d\n\n", pkt.header.seq_num);
                    }
                    else{
                        // out of order — store but don't ACK
                        printf("The expected SEQ NUM is %d\n\n",sm->sockets[i].rwnd.exptd_seq);
                    }
                    
                }
               /*  else if(pkt.header.type == ACK){
                    uint8_t ack_no = pkt.header.seq_num;
                    uint8_t new_rwnd = pkt.header.rwnd;
                    sm->sockets[i].swnd.acked_wnd_size = new_rwnd;
                    printf("acked window size : %d\n",new_rwnd);

                    // slide window 
                    while(sm->sockets[i].swnd.cnt > 0){
                        //int idx = sm->sockets[i].swnd.start % BUF_SIZE;
                        //uint8_t seq = sm->sockets[i].send_buf.msg[idx].header.seq_num;

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
                    printf("After ACK: swnd.cnt=%d send_buf.cnt=%d\n",
                        sm->sockets[i].swnd.cnt,
                        sm->sockets[i].send_buf.cnt);
                }*/
                else if(pkt.header.type == ACK){
                        uint8_t ack_no = pkt.header.seq_num;
                        uint8_t new_rwnd = pkt.header.rwnd;
                        sm->sockets[i].swnd.acked_wnd_size = new_rwnd;
                        printf("acked window size : %d\n", new_rwnd);

                        // duplicate ACK check — if swnd is empty, nothing to slide
                        if(sm->sockets[i].swnd.cnt == 0){
                            printf("Duplicate ACK — swnd empty, just updated wnd_size\n");
                        } else {
                            // slide window
                            while(sm->sockets[i].swnd.cnt > 0){
                                uint8_t s = sm->sockets[i].send_buf.msg[
                                    sm->sockets[i].swnd.start % BUF_SIZE
                                ].header.seq_num;

                                sm->sockets[i].swnd.start = (sm->sockets[i].swnd.start + 1) % BUF_SIZE;
                                sm->sockets[i].send_buf.head = (sm->sockets[i].send_buf.head + 1) % BUF_SIZE;
                                sm->sockets[i].send_buf.cnt--;
                                sm->sockets[i].swnd.cnt--;

                                if(s == ack_no) break;
                            }
                        }
                        printf("After ACK: swnd.cnt=%d send_buf.cnt=%d\n",
                            sm->sockets[i].swnd.cnt,
                            sm->sockets[i].send_buf.cnt);
                    }
            }
            pthread_mutex_unlock(&sm->lock);
        }
    }
}

void* S(void *arg){
    //AVINASH
    printf("S thread running\n");
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
                        printf("Thread S: Retransmitted from send_buf, cnt = %d\n", sm->sockets[i].send_buf.cnt);
                        int udpsock = sm->sockets[i].udp_sockfd;
                        struct sockaddr_in dst = sm->sockets[i].dest_addr;
                        socklen_t dstlen = sizeof(struct sockaddr_in);

                        sendto(udpsock,&pkt,l,0,(struct sockaddr*)&dst,dstlen);
                        //reset timer
                        sm->sockets[i].swnd.send_time[j] = time(NULL);
                    }
                }
            }

            int eff_wnd = min(sm->sockets[i].swnd.wnd_size,sm->sockets[i].swnd.acked_wnd_size);
            printf("Thread S: Socket number: %d Eff window size:%d Sender Win Size:%d ACKed Win Size:%d\n",
                i,eff_wnd,sm->sockets[i].swnd.wnd_size,sm->sockets[i].swnd.acked_wnd_size);

            while ((sm->sockets[i].swnd.cnt < eff_wnd) &&
                (sm->sockets[i].send_buf.cnt > sm->sockets[i].swnd.cnt)){
                int idx = (sm->sockets[i].send_buf.head + sm->sockets[i].swnd.cnt) % BUF_SIZE;

                ktp_packet pkt = sm->sockets[i].send_buf.msg[idx];
                int l = sizeof(pkt);

                int udpsock = sm->sockets[i].udp_sockfd;
                struct sockaddr_in dst = sm->sockets[i].dest_addr;
                socklen_t dstlen = sizeof(struct sockaddr_in);
                sendto(udpsock,&pkt,l,0,(struct sockaddr*)&dst,dstlen);
                printf("Thread S: sent DATA for seq %d\n",pkt.header.seq_num);
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
    signal(SIGINT, sig_handler);

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