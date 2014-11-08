/*   runner3   runner2   runner1   distor1
        |         |         |<--------|       dist_req   
        |         |<------------------|       dist_req   
        |<----------------------------|       dist_req
        |         |         |-------->|       dist_ack
        |         |------------------>|       dist_ack
        |---------------------------->|       dist_ack
        |         |         |<--------|       dist_run
        |         |<------------------|       dist_run
        |<----------------------------|       dist_run
 */

#define k_dist_req "dist_req"
#define k_dist_ack "dist_ack"
#define k_dist_run "dist_run"
#define k_dist_run_finished "dist_run_finished"

////////////////////////////////////////////////////////////////////////////////
struct Job {
  Job() { reset(); }
  
  string name;
  bool is_start;
  string distor_ip;
  string distor_id;
  set<string> runner_ips;
  size_t finished_runner_count;

  bool is_done() const {
    return finished_runner_count == runner_ips.size();
  }
  
  void dump_runers() {
    cout << "current runners:" << endl;
    for (set<string>::iterator i = runner_ips.begin(); i != runner_ips.end(); ++i)
      cout << *i << endl;
  }
  
  void reset() {
    name = "";
    is_start = false;
    distor_ip = "";
    distor_id = "";
    runner_ips.clear();
    finished_runner_count = 0;
  }
} g_job;

struct Node {
  string id() {
    static string id_ = uuid();
    return id_;
  }
  
  set<string> ips_;

  int runner_index_in(const set<string>& runners) {
    typedef set<string>::const_iterator set_cit;
    for (set_cit i = this_node.ips_.begin(); i != this_node.ips_.end(); ++i) {
      size_t index = 0;
      for (set_cit r = runners.begin(); r != runners.end(); ++r, ++index) {
        if (*i == *r)
          return index;
      }
    }
    return -1;
  }
  
  void reset() {
    ips_.clear();
  }

  bool is_distor() {
    return g_job.distor_id == id();
  }
  
} this_node;

void log(const string& log) {
  HANDLE h = GetStdHandle(STD_OUTPUT_HANDLE); 
  if (h == INVALID_HANDLE_VALUE) 
      return;
  
  SetConsoleTextAttribute(h, FOREGROUND_GREEN);
  cout << log << endl;
  SetConsoleTextAttribute(h, FOREGROUND_RED | FOREGROUND_GREEN | FOREGROUND_BLUE);
}

void reset_node_status() {
  log(string("========== job:[") + g_job.name + "] finished. ==========");
  this_node.reset();
  g_job.reset();
}

bool is_busy() {
  return g_job.is_start || ! g_job.name.empty();
}

////////////////////////////////////////////////////////////////////////////////
struct Dmsg : Msg {
  Dmsg(const string& cmd, const string&job) : io_(NULL) {
    attr("node", this_node.id());
    attr("cmd", cmd);
    attr("job", job);
  }
  
  Dmsg(const string& m) : Msg(m), io_(NULL) {/**/}
  Dmsg(struct iobuf *io) : Msg(string(io->buf, io->len)), io_(io) {/**/}

  ~Dmsg() {
    if (io_)
      iobuf_remove(io_, io_->len);
  }

  bool is(const string& v)  {
    return 0 == strcmp((*this).operator[]("cmd"),  v.c_str());
  }
  
  bool is_job(const string& v)  {
    return 0 == strcmp((*this).operator[]("job"),  v.c_str());
  }
  
  bool is_from_node(const string& v) {
    return 0 == strcmp((*this).operator[]("node"), v.c_str());
  }

  void broadcast() {
    udp_broadcast(content_, 9999);
  }
  
  struct iobuf *io_;
};

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
void* dist_job_thread_func(void*) {
  Dmsg(k_dist_req, g_job.name).broadcast();
  sleep(2);
  if (g_job.runner_ips.empty()) {
      printf("find no runners for job:%s.\n", g_job.name.c_str());
      reset_node_status();
      return NULL;
  }
  
  g_job.dump_runers();
  Dmsg(k_dist_run, g_job.name).broadcast();

  int i = 0;
  while (1) {
    if (g_job.is_done()) {
      printf("job:%s is done.\n", g_job.name.c_str());
      break;
    } else if ( 0 == ++i % 6 ) {
      printf("waiting runner to done for job:%s.\n", g_job.name.c_str());
    }
    sleep(10);
  }
  
  reset_node_status();
  return NULL;
}

int handle_job_log_query_as_distor(struct mg_connection *conn, const string& uri, const char* job) {
  if ( uri != "/__job_log_as_distor" || strlen(job) == 0)
    return MG_FALSE;

  string html = "<html><body>";
  for (set<string>::iterator it = g_job.runner_ips.begin(); it != g_job.runner_ips.end(); ++it) {
    html += string_format(
      "<p>log on runner :%s</p>"
      "<p><iframe width='1000px' height='380px' src='http://%s:9000/__job_log_as_runner?job=%s'></iframe></p>"
      , it->c_str(), it->c_str(), job);
  }
  html += "</body></html>";
  
  mg_printf_data(conn, html.c_str());
  return MG_MORE; 
}

int handle_job_log_query_as_runner(struct mg_connection *conn, const string& uri, const char* job) {
  if ( uri != "/__job_log_as_runner" || strlen(job) == 0)
    return MG_FALSE;

  const char* node_id = this_node.id().c_str();
  string html = string_format(
    "<html><head>                                                                  "
    "    <title>dist_test</title>                                                  "
    "    <script src='jquery-1.11.0.min.js'></script>                              "
    "    <script language='javascript' type='text/javascript'>                     "
    "        var start = 0;                                                        "
    "        function __func_to_load_from(url__, start__) {                        "
    "            var __func = function() {                                         "
    "                $.ajax({                                                      "
    "                    url: url__,                                               "
    "                    headers: {Range: 'bytes=' + start__ + '-'},               "
    "                    success: function( rsp ) {                                "
    "                        start += rsp.length;                                  "
    "                        $('#dist_log').html($('#dist_log').html() + rsp);     "
    "                        var delay = rsp.length == 0 ? 5000 : 1000;            "
    "                        setTimeout(__func_to_load_from(url__, start), delay); "
    "                    }                                                         "
    "                }).fail(function() { setTimeout(__func, 5000); });            "
    "            };                                                                "
    "            return __func;                                                    "
    "        };                                                                    "
    "                                                                              "
    "        __func_to_load_from('%s/__dist/run.log', 0)();                        "
    "    </script>                                                                 "
    "</head>                                                                       "
    "<body>                                                                        "
    "<span                                                                         "
    "  style=\"white-space:pre; font-family: 'Lucida Console', Monaco, monospace;\""
    "  id='dist_log'></span>                                                       "
    "</body></html>                                                                "
    , job);
  
  mg_printf_data(conn, html.c_str());
  return MG_MORE;
}



int handle_distrun_req(struct mg_connection *conn, const string& uri, const char* job) {
  if ( uri != "/__distrun" || strlen(job) == 0) {
      return MG_FALSE;
  }

  if (is_busy()) {
    send_http_error(MG_CONN_2_CONN(conn), 423, "only support one job at same time.");
    printf("only support one job at same time. ignore request:%s\n", uri.c_str());
    return MG_TRUE;
  }

  g_job.name = job;
  g_job.distor_id = this_node.id();

  string html = string_format("<html><body>"
    "<p> wait job to start ...</p>"
    "<script>setTimeout(function() { window.location.href = 'http://localhost:9000/__job_log_as_distor?job=%s'; }, 5000);</script>"
    "</body></html>", job);
  mg_printf_data(conn, html.c_str());
  
  ns_start_thread(dist_job_thread_func, NULL);
  return MG_MORE;
}

int handle_list_dir_req(struct mg_connection *conn, const string& uri) {
  if ( ! ends_with(uri, "/"))
    return MG_FALSE;
  
  string dir = uri.substr(1, uri.size() - 1);
  if (dir.empty()) {
    dir = ".";
  }
  printf("will sending directory list for: %s\n", dir.c_str());
  send_directory_listing(MG_CONN_2_CONN(conn), dir.c_str());
  return MG_TRUE;
}

static int http_req_handler(struct mg_connection *conn, enum mg_event ev) {
  if (MG_AUTH == ev)
    return MG_TRUE;
  
  if (MG_REQUEST == ev) {
    string uri(conn->uri);
    int ret;
    
    char param_job[1024];
    mg_get_var(conn, "job", param_job, sizeof(param_job));

    if (  MG_FALSE != (ret = handle_distrun_req(conn, uri, &param_job[0]))
       || MG_FALSE != (ret = handle_job_log_query_as_distor(conn, uri, &param_job[0]))
       || MG_FALSE != (ret = handle_job_log_query_as_runner(conn, uri, &param_job[0]))
       || MG_FALSE != (ret = handle_list_dir_req(conn, uri)))
      return ret;

    if (uri.size() > 1) {
      mg_send_file(conn, uri.substr(1).c_str(), s_no_cache_header);
    } else {
      mg_send_file(conn, "index.html", s_no_cache_header);
    }
    
    return MG_MORE;
  }

  return MG_FALSE;
}

void* fetch_tests_and_run(void*) {
  int total_shard = g_job.runner_ips.size();
  int shard_index = this_node.runner_index_in(g_job.runner_ips);
  
  string cmd = string_format(
    "wget -r -np -nH http://%s:9000/%s/ && %%CD%%/%s/run_job.bat %d %d"
    , g_job.distor_ip.c_str(), g_job.name.c_str(), g_job.name.c_str(), total_shard, shard_index);
  
  printf("* start fetch job:%s, cmd:%s.\n", g_job.name.c_str(), cmd.c_str());
  system(cmd.c_str());
  printf("run cmd done: %s\n", cmd.c_str());

  Dmsg(k_dist_run_finished, g_job.name).broadcast();
  
  reset_node_status();
  return NULL;
}

static void udp_handler(struct ns_connection *nc, int ev, void *) {
  struct iobuf *io = &nc->recv_iobuf;
  switch (ev) {
    case NS_RECV:{
      Dmsg msg(io);
      bool is_looped_msg = msg.is_from_node(this_node.id());
        
      if (is_looped_msg)
          this_node.ips_.insert(peer_ip(nc));
      
      printf("--- udp RECV: %s from [%s]\n", msg.c_str(), peer_ip(nc).c_str());
      
      if (msg.is(k_dist_req)) {
        if (is_busy()) {
          printf("--- busy on job:%s. ignore: %s\n", g_job.name.c_str(), msg.c_str());
          return;
        }

        g_job.name = msg["job"];
        g_job.distor_ip = peer_ip(nc);

        Dmsg(k_dist_ack, g_job.name).broadcast();
        return;
      }

      if (msg.is_job(g_job.name)) {
        if (msg.is(k_dist_run) && ! is_looped_msg) {
          g_job.is_start = true;
          printf("* start running job:%s.\n", g_job.name.c_str());
          g_job.dump_runers();
          ns_start_thread(fetch_tests_and_run, NULL);
          return;
        }
  
        if (msg.is(k_dist_ack) && !g_job.is_start) {
          printf("confirm will run job on: %s\n", peer_ip(nc).c_str());
          g_job.runner_ips.insert(peer_ip(nc));
          return;
        }

        if (msg.is(k_dist_run_finished) && this_node.is_distor()) {
          ++g_job.finished_runner_count;
          printf("job:%s finished on: %s, finished count:%d\n", msg["job"], peer_ip(nc).c_str(), g_job.finished_runner_count);
          return;
        }
      }
      
      break;
    }
      
    default:
      printf("unknown ev: %d\n", ev);
      break;
  }
}

int main(void) {
  struct mg_server* server = mg_create_server(NULL, http_req_handler);
  mg_set_option(server, "listening_port", "9000");
  ns_bind(&server->ns_mgr, "udp://0.0.0.0:9999", udp_handler, NULL);
  log(string("Starting on http:9000, udp:9999, id:") + this_node.id());
  
  for (;;)
    mg_poll_server(server, 100);

  mg_destroy_server(&server);
  return 0;
}

