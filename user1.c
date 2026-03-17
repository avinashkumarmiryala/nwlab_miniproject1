#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <sys/select.h>

#define N 256
int main(){
    init();
    int sockfd = k_socket(AF_INET,SOCK_KTP,0);
    
    const char* myip = "127.0.0.1";
    const char* dstip = "127.0.0.1";
    int my_port = 5000;
    int dst_port = 6000;

    struct sockaddr_in myaddr;
    myaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    myaddr.sin_family = AF_INET;
    myaddr.sin_port = htons(5000);

    struct sockaddr_in dstaddr;
    dstaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    dstaddr.sin_family = AF_INET;
    dstaddr.sin_port = htons(6000);

    socklen_t dstlen = sizeof(dstaddr);

    int b = k_bind(sockfd,myip,my_port,dstip,dst_port);
    if(b==-1){
        printf("Bind Error!\n");
        return 0;
    }

    while(1){
        printf("Enter the input:");
        char sbuf[N];
        fgets(sbuf,N,stdin);
        sbuf[strcspn(sbuf,"\n")] = '\0';
        
        k_sendto(sockfd,sbuf,strlen(sbuf)+1,0,(struct sockaddr*)&dstaddr,dstlen);
        if(!strcmp("QUIT",sbuf)) break;

        char rbuf[N];
        k_recvfrom(sockfd,rbuf,sizeof(rbuf),0,(struct sockaddr*)&dstaddr,&dstlen);
        printf("The response is %s",rbuf);
    }
    k_close(sockfd);
    return 0;
}