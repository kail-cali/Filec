#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>

#include <pthread.h>
#include <dirent.h>
#define err(str) fprintf(stderr, str)



#define max(a,b) (((a)<(b))?(a):(b))
#define min(a,b) (((a)>(b))?(a):(b))

/*Global*/

static volatile int SERVICE_KEEPALIVE;  // Service status used for run forever or terminate program
const int MAX_EVNET = 1024;

/*----------class-----------*/

typedef struct Control{
    int service_status;    

    int port;
    char inet[16];
    int num_worker;
    int session_timeout;
    int epoll_timeout;
    int max_epoll_event;
    /**/
    int num_lock;

    char root[20];



} Control;

typedef struct Event{
    int event_id;
    struct epoll_event event;
    struct sockaddr_in client_addr;

} Event;



typedef struct File{
    /*
    Object for transfering query-result to client


     */
    char name[50];
    unsigned char contents_b[1024];
    int size;
    int find;
} File;

typedef struct Session{


    /*
    To manage client-session 
    
    Includeded with "client fd", "worker id which handling sesseion", and read buffer

    */
    uintptr_t session_iid;
    int worker_id;



    char read_buffer[1024];
    int varlead_status;
    
} Session;

typedef struct BinSemaphore{
    /*
     Binary-semaphore for handling thread and lock
     0: there is no job
     1: new jobs have created
     */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int v;
} BinSemaphore;


typedef struct Job{
    /*
    Linked list for indexing job
    Working functionaly -->  func(session)
    
    prev:: linked list
    function:: function for processing task, in this cases, functions do read stream, find file and send stream 
    info:: args for function, managed with session object
     */
    struct Job* prev; 
    void (*function )(void* args);
    Session* info;
} Job;

typedef struct JobScheduler{
    pthread_mutex_t rxmutex;

    Job* front;
    Job* rear;
    int num_lock;
    int len;
    BinSemaphore** has_job;
//job scheduler 

} JobScheduler;

typedef struct JobQueue{
    /*
     Queue object for manage job-linked-list, works with two pointer
    
     Notifiy new-job to worker-thread  with job queue and binary semaphore
     
     if new job has come, submited by Cluster shaped of Session. and broadcast them to all alive-work-thread


     len:: left job
     has_job:: lock for job queue. broadcast to all thread
     
     */
    pthread_mutex_t rmutex;
    Job* front;
    Job* rear;

    int len;
    BinSemaphore* has_job;

} JobQueue;


typedef struct Worker{
    /*
     Child thread at the thread-pool
     Processing session functionaly
     
    (v0.1)Wait until new job is allocated, and it is done with one single lock
    (v0.2)Wait until new job is allcoated, and each thread have different bucket of lock

    -----------------------------------------------------------------------
     id:: logical id for worker
     cluster:: pointer for main thread-pool
    
     
     */
    pthread_t pthread;
    int id;

    int semaphore_id; // logical index for mutex lock

    struct Cluster* cluster;


} Worker;


typedef struct Cluster{
    /*
     Main thread-pool
     - Create stream
     - Create worker and manage them with "num_alive_worker" and "num_working_worker""
     - Handleing accpet process
     - Have global lock used for idle all thread or terminate thread

     --------------------------------------------------------------    
     main_thread :: thread_t
     lock:: global lock for shotdown, idle, terminate all worker
     idle:: lock for wait signal

     wokers:: pointer array for Worker-object

     num_worker:: number of worker thread, currently it is static from start
     num_alive_worker:: number of alive but not-working thread
     num_working_worker:: number of working thread

     job_queue:: handler for job-liked-list

     server_fd:: server's stream fd
     serv_addr:: server socket
     port:: open port number
     inet:: open server address

     
     */
    struct Control* control;

    pthread_t main_thread;
    pthread_mutex_t lock;
    pthread_cond_t idle;

    Worker** workers;
    int num_worker;
    int num_alive_worker;
    int num_working_worker;
    JobQueue job_queue;

    JobScheduler scheduler;// ###(nf) job-scheduling

    int server_fd;
    struct sockaddr_in serv_addr;
    int port;


} Cluster;


/*--------------------------*/

/*-----------call------------*/
struct Cluster;
struct Worker;

static int worker_init(Cluster* cluster, struct Worker** thread_p, int id);

static int submit(Cluster* cluster_p, void(*function_p)(void* ), void* args_p );

void task_fn(void* args);


static void bsem_post(BinSemaphore* bsem_p);

void test_task_fn(void* args);

struct File* find_file(char* file_name);


struct Session* create_session(int  new_session_id );
static int submit_with_session(Cluster* cluster, void(*function)(void*), void* session_p);
static int semaphore_init(JobScheduler* scheduler, struct BinSemaphore** node);
/*--------------------------*/


static void bsem_init(BinSemaphore* bsem_p, int value){

    pthread_mutex_init(&(bsem_p->mutex), NULL);
    pthread_cond_init(&(bsem_p->cond),NULL);
    bsem_p->v = value;
}

static void bsem_reset(BinSemaphore* bsem_p){
    bsem_init(bsem_p, 0);
}

static void bsem_post(BinSemaphore* bsem_p){
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p->v = 1;
    // ### post 가 두번 되는 건 아닌지 확인해야 한다
    pthread_cond_signal(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);
}

static void wait(BinSemaphore* bsem_p){
    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v != 1){
        pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
    }
    bsem_p->v = 0;
    // ### value 를 누가 바꾸어주는지 중요하다 지금처럼 worker  가 전부 바꾸고 POST 하는 동작은 위험할 수 있다
    pthread_mutex_unlock(&bsem_p->mutex);

}


/*--------------------------*/



static int job_queue_init(JobQueue* job_queue_p){
    job_queue_p->len =0;
    job_queue_p->front = NULL;
    job_queue_p->rear = NULL;
    job_queue_p->has_job = (struct BinSemaphore* )malloc(sizeof(struct BinSemaphore ));
    
    if (job_queue_p->has_job == NULL){
        err("job_queue_init():: could not allocate memory for binary semaphore in job queue");
        return -1;
    }

    pthread_mutex_init(&(job_queue_p->rmutex), NULL);
    bsem_init(job_queue_p->has_job, 0);
    
    return 0;
}

static void push_back(JobQueue* job_queue_p, BinSemaphore* has_job, struct Job* new_job){
    
    pthread_mutex_lock(&job_queue_p->rmutex);
    new_job->prev = NULL;
    if (job_queue_p->len==0){
        job_queue_p->front = new_job;
        job_queue_p->rear = new_job;
    }
    if (job_queue_p->len>=1){
        job_queue_p->rear->prev = new_job;
        job_queue_p->rear = new_job;
    }
    job_queue_p -> len ++ ;
    bsem_post(has_job);

    pthread_mutex_unlock(&job_queue_p->rmutex);
}

static void push_back_cluster(JobScheduler* scheduler, BinSemaphore* has_job, struct Job* new_job){

    new_job->prev = NULL;
    


}

static struct Job* pop_front_cluster(JobQueue* job_queue){



}

static struct Job* pop_front(JobQueue* job_queue_p, BinSemaphore* has_job ){
    pthread_mutex_lock(&job_queue_p-> rmutex);
    Job* front_job = job_queue_p -> front;
    if (job_queue_p ->len ==0){

    }
    else if (job_queue_p->len==1){
        job_queue_p->front = NULL;
        job_queue_p->rear = NULL;
        job_queue_p -> len = 0;
    }
    else if (job_queue_p->len >= 2){
        job_queue_p -> front = front_job->prev;
        job_queue_p -> len --;
        bsem_post(has_job);
    }
    pthread_mutex_unlock(&job_queue_p->rmutex);
    return front_job;

}



static void* worker_do(Worker* thread_p){
    /*
    Process session while service alive,
    Wait until new job is allocated
    
     
     
     */
    char worker_name[16] = {0};
    snprintf(worker_name, 16, "worker#%d", thread_p->id);

    Cluster* cluster_p = thread_p->cluster;

    pthread_mutex_lock(&cluster_p->lock);
    cluster_p->num_alive_worker +=1;
    pthread_mutex_unlock(&cluster_p->lock);

    while (SERVICE_KEEPALIVE) {
        wait(cluster_p->job_queue.has_job);
        if (SERVICE_KEEPALIVE){

            pthread_mutex_lock(&cluster_p->lock);
            cluster_p->num_working_worker +=1;
            pthread_mutex_unlock(&cluster_p->lock);


            void(* fn_buf)(void* );
            void* args_buf;
            Job* job_p = pop_front(&cluster_p->job_queue, cluster_p->job_queue.has_job);
            if (job_p){
                job_p->info->worker_id = thread_p->id; 
                fn_buf = job_p->function;
                args_buf = job_p->info;
                
                fn_buf(args_buf);
                
                
                free(job_p->info);
                free(job_p);
            }
            /*temporerly give latnecy*/
            printf("sleep - latnecy::thraed[%s]  \n", worker_name);
            sleep(10);
            printf("wake up- latnecy::thraed[%s] \n", worker_name);
            /*delete later */
        }
        
        pthread_mutex_lock(&cluster_p->lock);
        cluster_p->num_working_worker -= 1;
        pthread_mutex_unlock(&cluster_p->lock);
    
    }

    pthread_mutex_lock(&cluster_p->lock);
    cluster_p->num_alive_worker -= 1;
    pthread_mutex_unlock(&cluster_p->lock);
    return NULL;
    
}

static void* cluster_do(Cluster* cluster){
    printf("\n==Cluster working on thread== \n");
    Control* control = cluster->control;

    /*<D>epoll init temp */
    int epoll_fd = epoll_create(control->max_epoll_event);
    if (epoll_fd <0 ){
        err("epoll init error\n ");

    }

    struct epoll_event event;
    //event = (struct epoll_event* )malloc(sizeof(struct epoll_event));

    event.events = EPOLLIN | EPOLLET;
    event.data.fd = cluster->server_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cluster->server_fd, &event) < 0){
        err("epoll init error\n");
    }

    struct epoll_event epoll_events[control->max_epoll_event];
//    epoll_events = (struct epoll_event* )malloc((control->max_epoll_event) * sizeof(struct epoll_event ));
    int batch4event;
    
    while (SERVICE_KEEPALIVE && cluster->server_fd >0){
        batch4event = epoll_wait(epoll_fd, epoll_events, control->max_epoll_event, control->session_timeout );

        if (batch4event <0){
            continue;

        }

        for (int i =0; i <batch4event; i++){
            if (epoll_events[i].data.fd==cluster->server_fd){
                int new_session;
                int session_len;
                /*<D> epoll init tmp*/
                struct sockaddr_in client_addr;

                session_len = sizeof(client_addr);
                new_session = accept(cluster->server_fd, (struct sockaddr* )&client_addr, (socklen_t* )&session_len);
                
                int flags = fcntl(new_session, F_GETFL);

                flags |= O_NONBLOCK;
                if (fcntl(new_session, F_SETFL, flags)<0){
                    printf("while creating new session[%d] fcntl() error \n", new_session);
                    continue;
                }
                if (new_session <0){
                    continue;
                }
                struct epoll_event event;
                event.events = EPOLLIN | EPOLLET;
                event.data.fd = new_session;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, new_session, &event)<0){
                    err("epoll ctl error\n");
                    continue;
                }

            }
             else {
                 int str_len;
                 int new_session = epoll_events[i].data.fd;
                 Session* session;
                 session = create_session(new_session);

                 str_len = read(session->session_iid, session->read_buffer, sizeof(session->read_buffer));
                
                 if (str_len==0){
                     printf("close session timeout [%d]\n", new_session);
                     close(new_session);
                     epoll_ctl(epoll_fd, EPOLL_CTL_DEL, new_session, NULL);

                 }
                 else{
                    submit_with_session(cluster, task_fn, (void* )(uintptr_t)session);
                 }


             }
         }

    }

}


static void* test_cluster_do(Cluster* cluster){
    /*
    Main thread pool
    - Accept client fd
    - Submit seesion


    */
// ### semaphore -> pipe노티파이 진행 

    printf("\n==TEST MODE==\n");
    int session_len = sizeof(cluster->serv_addr);
    int new_session; 
    char read_buffer[1024];
    int varlead_status;
    while (SERVICE_KEEPALIVE && cluster->server_fd >0){
        // ### epoll -> wait, manage n- accept 
        /* create session */
        new_session = accept(cluster->server_fd, (struct sockaddr* )&(cluster->serv_addr), &session_len);
        if (new_session <0){
            err("cluster- Accept error found\n");
        }
        /*create and submit session to job queue */
        
        submit(cluster, test_task_fn, (void* )(uintptr_t)new_session);

    }
}


static int submit_with_session(Cluster* cluster, void(*function)(void*), void* session_p){
    
    Job* new_job;
    new_job =(struct Job*)malloc(sizeof(struct Job));
    
    if (new_job==NULL){
        err("at submit():: could not allocate memory for new job\n");
        return -1;
    }
    
    Session* session;
    session = session_p;
    new_job->function = function;
    new_job->info = session;

    push_back(&cluster->job_queue, cluster->job_queue.has_job , new_job);
    return 0;

}



struct Session* create_session(int new_session_id ){
    
    Session* session;
    session = (struct Session* )malloc(sizeof(struct Session));
    if (session ==NULL){
    
        err("at submit():: could not allocate memory for new session\n");
        return NULL;
    }

    session->session_iid = (uintptr_t)new_session_id;
    return session;
}


static int submit(Cluster* cluster_p, void(*function_p)(void* ), void* args_p ){
    /*
     Create session and push job to job-queue
    
     - Create job which container for processing query
     - Create session wihch manage life-cycle of seesion, client fd, read-buffer
     - Pusch back job (contianer) into queue

     
     
     
     */
    /*mapping session and schedule at job queue */
    Job* new_job;
    
    new_job = (struct Job* )malloc(sizeof(struct Job));
    if (new_job==NULL){
        err("at submit():: could not allocate memory for new job\n");
        return -1;
    }
         
    /*map session */
    Session* session_info;
    session_info = (struct Session* )malloc(sizeof(struct Session));
    if (session_info ==NULL){
    
        err("at submit():: could not allocate memory for new session\n");
        return -1;
    }
    session_info->session_iid = (uintptr_t) args_p;
    session_info->varlead_status;
    

    /*process task at function */
     
    new_job->function = function_p;
    new_job->info = session_info;
    
    push_back(&cluster_p->job_queue, cluster_p->job_queue.has_job , new_job);
    return 0;

}
void task_fn(void* args){
    /*
     task fn
     - create file fd
     - find file from server
     - send file to client 

     */

    Session* session = (Session* )args;
    File* file;
    printf("Thread #%d (%u)  Working on session[%ld] \n", session->worker_id , (int)pthread_self(), session->session_iid);
    
    printf("\t: T[%d] recv msg from Session[%ld] ::  %s \n",session->worker_id, session->session_iid,  session->read_buffer);
    
    file = find_file(session->read_buffer);
        
    if (file==NULL){
        char no_file_msg[1024] = {0};
        snprintf(no_file_msg, 1024, "<csf>such a file name doesn't exists");
        send(session->session_iid, no_file_msg, strlen(no_file_msg),0); 
        printf("\t:NO FILE::  T[%d] send msg to Session[%ld] \n",session->worker_id ,session->session_iid);
     }  
     else{
          send(session->session_iid, file->contents_b, sizeof(file->contents_b),0);
        printf("\t: T[%d] send msg to Session[%ld] \n",session->worker_id ,session->session_iid);
        free(file);
        
     }
}






void test_task_fn(void* args){
    /*
    Task_fn is where the actual  work is done
    
    Be carried by job until worker thread allocated
    
    - Create file fd
    - Read steam
    - Find file from server
    - Send file to client

    
     
     
     */
    /*procceing session :: read find write send */
    int timeout = 0;
    
    Session* session_p = (Session* )args;
    File* file_p;
    printf("Thread #%d (%u)  Working on session[%ld] \n", session_p->worker_id , (int)pthread_self(), session_p->session_iid);

    while (timeout==0){

 
        session_p->varlead_status = read(session_p->session_iid, session_p->read_buffer, 1024);    
         printf("\t: T[%d] recv msg from Session[%ld] ::  %s \n",session_p->worker_id, session_p->session_iid,  session_p->read_buffer);
        

        file_p = find_file(session_p->read_buffer);   // find file    
        
        if (file_p==NULL){
            char no_file_msg[1024] = {0};
            snprintf(no_file_msg, 1024, "<csf>such a file name doesn't exists");
            send(session_p->session_iid, no_file_msg, strlen(no_file_msg),0);
            printf("\t:NO FILE::  T[%d] send msg to Session[%ld] \n",session_p->worker_id ,session_p->session_iid);
        }  
        else{

            send(session_p->session_iid, file_p->contents_b, sizeof(file_p->contents_b),0);
            printf("\t: T[%d] send msg to Session[%ld] \n",session_p->worker_id ,session_p->session_iid);
            free(file_p);
        }
        timeout = 1;
    }
}


static int job_scheduler_init(JobScheduler* scheduler, int num_lock){
    /*
    
     */
    scheduler->len = 0;
    scheduler->front = NULL;
    scheduler -> rear = NULL;
    scheduler->num_lock = num_lock;

    scheduler -> has_job = (struct BinSemaphore** )malloc(num_lock*sizeof(struct BinSemaphore* ));;

    for (int i=0; i< num_lock; i++){
        semaphore_init(scheduler, &scheduler->has_job[i]); 
        
    }

    return 0;
}


static int semaphore_init(JobScheduler* scheduler, struct BinSemaphore** node){
    *node = (struct BinSemaphore* )malloc(sizeof(struct BinSemaphore ));
    bsem_init((*node), 0);


    return 0;
}

static int worker_init(Cluster* cluster, struct Worker** thread_p, int id){
    *thread_p = (struct Worker*)malloc(sizeof(struct Worker));
    if (thread_p==NULL){
        err("worker_init():: allocate worker on memory failed");
        return -1;
    }
    
    (*thread_p) -> cluster = cluster;
    (*thread_p) -> id = id;
    pthread_create(&(*thread_p)->pthread, NULL, (void * (*)(void* )) worker_do, (*thread_p));
    pthread_detach((*thread_p)->pthread);

    return 0;
}


static int epoll_init(Cluster* cluster, int server_fd){
    
    return 0;

}


static int _stream_init(Cluster** cluster_p){
    Cluster* cluster = (*cluster_p);
    cluster->port = 32209;
    int opt = 6;
    int server_fd;
    
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd <0){
        perror("stream_init() socket create failed \n");
        return -1;
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
        perror("set socket opt\n");
        return -1;
    }
    cluster->serv_addr.sin_family=AF_INET;
    cluster->serv_addr.sin_addr.s_addr = INADDR_ANY;
    cluster->serv_addr.sin_port = htons(cluster->port);
    if (bind(server_fd, (struct sockaddr*)&(cluster->serv_addr), sizeof((cluster->serv_addr)))<0){
        err("stream_init : Bind Failed\n");
        return -1;
    }

    if (listen(server_fd, 3)<0){
        perror("listen Failed\n");
        return -1;
    }

    cluster -> server_fd = server_fd;
    if (epoll_init(cluster, server_fd)<0){
        perror("set epoll Failed\n");
        return -1;
    }
    return 0;
}


struct File* find_file(char* file_name){
    
    /*
    Find file with I/O 
    - Find dir from root
    - Find same length of file name
    - Configure while target and exist name is same
    - If does, read to memory buffer
    - Return with file struct
    - If not, return NULL
     */

    File* file;

    char file_path[1024] = {0};
    /*#hard coding */
    char* hard_path = "./server/book_file/";
    snprintf(file_path, strlen(hard_path)+1,"%s", hard_path);
    file = (struct File*)malloc(sizeof(struct File));
    DIR* os_fd;
    struct dirent* connector;
    os_fd = opendir(hard_path);

    if (os_fd==NULL){
        err("dir does not exists\n");
        free(file);
        return NULL;
    }


    while ((connector = readdir(os_fd))!= NULL){
        char* d_name = connector->d_name;
        if (strlen(d_name) ==strlen(file_name)){
            int same=1;
            for (int i =0; i< strlen(d_name); i++){
                if (d_name[i]!= file_name[i]){
                    same = 0;
                    break;
                }
            }
            if (same){
                // write
                strcat(file_path, d_name);
                
                FILE* fptr;
                fptr = fopen(file_path, "r");
                
                if(fptr==NULL){
                    printf("\t at find file():: fptr error check file path %s \n", file_path);
                    break;
                }
              
                fread((file->contents_b), 1, sizeof(file->contents_b), fptr);

                snprintf(file->name,50, "%s", d_name);
                file->find = 1;
                fclose(fptr);
                return file;
            }
        }
    }

    
    free(file);
    return NULL;
}






struct Cluster* cluster_init(Control* control){
    /*
    Allocate object to memory
    First create all the object at the process
    If all works done, give them to thread

    1) create cluster
    2) create job-queue
    3) create stream
    4) create worker -> not a terminate condition
    
     
     */

    Cluster* cluster;
    cluster = (struct Cluster* )malloc(sizeof(struct Cluster ));
    if (cluster==NULL){
        err("cluster_init():: allocatate cluster on memory failed\n");
        return NULL;
    } 
    
    cluster->control = control;
    cluster->num_worker = control->num_worker;
    int num_worker = control->num_worker;
    /*create job queue, terminate condition */
    if (job_queue_init(&cluster->job_queue) < 0){
        err("could not allocate job queue ");
        /*free here */
        free(cluster);
        return NULL;
    }
    
    
    /*###<nf> job scheduler */
    if (job_scheduler_init(&cluster->scheduler, cluster->num_worker) <0){
        free(cluster);
        return NULL;
    } 

    printf("--debug-- <job-scheudler> num lock %d \n", cluster->scheduler.num_lock);
    printf("--debug-- <job-scheudler> lock 1-idx %ld \n", (uintptr_t)(cluster->scheduler.has_job[0]));
    printf("--debug-- <job-scheduler> allocate\n");
     
    cluster->workers = (struct Worker** )malloc(num_worker * sizeof(struct Worker* ));
    if (cluster->workers==NULL){

        err("cluster_init():: allocatate worker  on memory failed\n");
        return NULL;
    }
    
    /*create stream on cluster*/
    if (_stream_init(&cluster)<0){
        err("|_create stream Failed\n");
        return NULL;
    }
    /*create lock*/ 
    pthread_mutex_init(&(cluster->lock), NULL);
    pthread_cond_init(&cluster->idle, NULL);
    /*create worker thread */
    for (int i=0; i<num_worker; i++){
        worker_init(cluster, &cluster->workers[i], i );
        printf("cluster init : Work thread %d created\n", i);
    }

     
    return cluster;
}


static int control_init(Control* control){
    /*
     
     I/O from server/control.txt
     */
    FILE* fptr;
    // hard coding --> add  argparse later version
    char* root = "./server/control.txt";
    
    fptr = fopen(root, "a+");
    char key[20];
    char value[20];
    while ( fscanf(fptr, "%s\t%s\n", key, value) != EOF ){
        if (strcmp(key, "port")==0){
            control->port = atoi(value);
        }
        else if (strcmp(key, "inet")==0){
        }
        else if (strcmp(key, "num_worker")==0){
            control->num_worker = atoi(value);
        }
        else if (strcmp(key, "session_timeout")==0){
            control->session_timeout = atoi(value);
        }
        else if (strcmp(key, "epoll_timeout")==0){
            control->epoll_timeout = atoi(value);
        }
        else if (strcmp(key, "max_epoll_event")==0){
            control -> max_epoll_event = atoi(value);
        }
        else if (strcmp(key, "root")==0){
            strcpy(control->root, value);
        }
        
    }
    
    
    

    return 0;

}


int main(){
    Control* control;
    if ( control_init(control) <0){
        return -1;
    }
    SERVICE_KEEPALIVE =1;

    Cluster* cluster = cluster_init(control);
    
    // ###->&&& 왜 쓰레드를 나중에 만들었는지 
    // ### cluster 가 worker 들을 매니징할 수 있는 지 아니면 demon thread 같은 개념인지
    pthread_create(&(cluster->main_thread), NULL, (void * (*)(void* )) cluster_do, cluster);

    pthread_detach(cluster->main_thread);

    while ( SERVICE_KEEPALIVE){
        // wait
        continue;

        }
    printf("----Service Stop----");
    return 0;
}
