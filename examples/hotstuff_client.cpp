/**
 * Copyright 2018 VMware
 * Copyright 2018 Ted Yin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <random>
#include <memory>
#include <signal.h>
#include <sys/time.h>


#include "hotstuff/database.h"
#include "hotstuff/in_memory_db.cpp"
#include "hotstuff/helper.h"


#define CPU_FREQ 2.2

#include "salticidae/type.h"
#include "salticidae/netaddr.h"
#include "salticidae/network.h"
#include "salticidae/util.h"

#include "hotstuff/util.h"
#include "hotstuff/type.h"
#include "hotstuff/client.h"

using salticidae::Config;

using hotstuff::ReplicaID;
using hotstuff::NetAddr;
using hotstuff::EventContext;
using hotstuff::MsgReqCmd;
using hotstuff::MsgRespCmd;
using hotstuff::CommandDummy;
using hotstuff::HotStuffError;
using hotstuff::uint256_t;
using hotstuff::opcode_t;
using hotstuff::command_t;

EventContext ec;
ReplicaID proposer;
size_t max_async_num;
int max_iter_num;
uint32_t cid;
uint32_t cnt = 0;
uint32_t nfaulty;


uint32_t table_size =  20000;
double denom = 0;
double g_zipf_theta = 0.5;
double zeta_2_theta;
uint64_t the_n;

myrand *mrand;

struct Request {
    command_t cmd;
    size_t confirmed;
    salticidae::ElapsedTime et;
    Request(const command_t &cmd): cmd(cmd), confirmed(0) { et.start(); }
};

using Net = salticidae::MsgNetwork<opcode_t>;

std::unordered_map<ReplicaID, Net::conn_t> conns;
std::unordered_map<const uint256_t, Request> waiting;
std::vector<NetAddr> replicas;
std::vector<std::pair<struct timeval, double>> elapsed;
std::unique_ptr<Net> mn;





uint64_t zipf(uint64_t n, double theta)
{
    assert(this->the_n == n);
    assert(theta == g_zipf_theta);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) /
                 (1 - zeta_2_theta / zetan);
    //	double eta = (1 - pow(2.0 / n, 1 - theta)) /
    //		(1 - zeta_2_theta / zetan);
    double u = (double)(mrand->next() % 10000000) / 10000000;
    double uz = u * zetan;
    if (uz < 1)
        return 1;
    if (uz < 1 + pow(0.5, theta))
        return 2;
    return 1 + (uint64_t)(n * pow(eta * u - eta + 1, alpha));
}


uint64_t get_server_clock()
{
#if defined(__i386__)
    uint64_t ret;
	__asm__ __volatile__("rdtsc"
						 : "=A"(ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
	__asm__ __volatile__("rdtsc"
						 : "=a"(lo), "=d"(hi));
	uint64_t ret = ((uint64_t)lo) | (((uint64_t)hi) << 32);
	ret = (uint64_t)((double)ret / CPU_FREQ);
#else
    timespec *tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
    delete tp;
#endif
    return ret;
}










void connect_all() {
    for (size_t i = 0; i < replicas.size(); i++)
        conns.insert(std::make_pair(i, mn->connect_sync(replicas[i])));


    mrand = (myrand *) new myrand();

    mrand->init(get_server_clock());



    zeta_2_theta = zeta(2, g_zipf_theta);
    the_n = table_size - 1;
    denom = zeta(the_n, g_zipf_theta);    
}

bool try_send(bool check = true) {
    if ((!check || waiting.size() < max_async_num) && max_iter_num)
    {


        int g_zipf_theta = 0.5;
        int temp_key = zipf(table_size - 1, g_zipf_theta);

        mrand->next();
        int temp_value = 2;


        auto cmd = new CommandDummy(cid, cnt++, temp_key, temp_value);
        MsgReqCmd msg(*cmd);
        for (auto &p: conns) mn->send_msg(msg, p.second);
//#ifndef HOTSTUFF_ENABLE_BENCHMARK
        HOTSTUFF_LOG_INFO("send new cmd %.10s",
                            get_hex(cmd->get_hash()).c_str());
//#endif
        waiting.insert(std::make_pair(
            cmd->get_hash(), Request(cmd)));
        if (max_iter_num > 0)
            max_iter_num--;
        return true;
    }
    return false;
}

void client_resp_cmd_handler(MsgRespCmd &&msg, const Net::conn_t &) {
    auto &fin = msg.fin;
    HOTSTUFF_LOG_DEBUG("got %s", std::string(msg.fin).c_str());
    const uint256_t &cmd_hash = fin.cmd_hash;
    auto it = waiting.find(cmd_hash);
    auto &et = it->second.et;
    if (it == waiting.end()) return;
    et.stop();
    if (++it->second.confirmed <= nfaulty) return; // wait for f + 1 ack
    HOTSTUFF_LOG_INFO("got %s, wall: %.3f, cpu: %.3f",
                        std::string(fin).c_str(),
                        et.elapsed_sec, et.cpu_elapsed_sec);
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    elapsed.push_back(std::make_pair(tv, et.elapsed_sec));

    waiting.erase(it);
    while (try_send());
}

std::pair<std::string, std::string> split_ip_port_cport(const std::string &s) {
    auto ret = salticidae::trim_all(salticidae::split(s, ";"));
    return std::make_pair(ret[0], ret[1]);
}

int main(int argc, char **argv) {
    Config config("hotstuff.gen.conf");

    auto opt_idx = Config::OptValInt::create(0);
    auto opt_replicas = Config::OptValStrVec::create();
    auto opt_max_iter_num = Config::OptValInt::create(100);
    auto opt_max_async_num = Config::OptValInt::create(10);
    auto opt_cid = Config::OptValInt::create(-1);
    auto opt_max_cli_msg = Config::OptValInt::create(65536); // 64K by default

    auto shutdown = [&](int) { ec.stop(); };
    salticidae::SigEvent ev_sigint(ec, shutdown);
    salticidae::SigEvent ev_sigterm(ec, shutdown);
    ev_sigint.add(SIGINT);
    ev_sigterm.add(SIGTERM);

    mn = std::make_unique<Net>(ec, Net::Config().max_msg_size(opt_max_cli_msg->get()));
    mn->reg_handler(client_resp_cmd_handler);
    mn->start();

    config.add_opt("idx", opt_idx, Config::SET_VAL);
    config.add_opt("cid", opt_cid, Config::SET_VAL);
    config.add_opt("replica", opt_replicas, Config::APPEND);
    config.add_opt("iter", opt_max_iter_num, Config::SET_VAL);
    config.add_opt("max-async", opt_max_async_num, Config::SET_VAL);
    config.add_opt("max-cli-msg", opt_max_cli_msg, Config::SET_VAL, 'S', "the maximum client message size");
    config.parse(argc, argv);
    auto idx = opt_idx->get();
    max_iter_num = opt_max_iter_num->get();
    max_async_num = opt_max_async_num->get();

    HOTSTUFF_LOG_INFO(" max_async_num is %d", int(max_async_num));
    std::vector<std::string> raw;
    for (const auto &s: opt_replicas->get())
    {
        auto res = salticidae::trim_all(salticidae::split(s, ","));
        if (res.size() < 1)
            throw HotStuffError("format error");
        raw.push_back(res[0]);
    }

    if (!(0 <= idx && (size_t)idx < raw.size() && raw.size() > 0))
        throw std::invalid_argument("out of range");
    cid = opt_cid->get() != -1 ? opt_cid->get() : idx;
    for (const auto &p: raw)
    {
        auto _p = split_ip_port_cport(p);
        size_t _;
        replicas.push_back(NetAddr(NetAddr(_p.first).ip, htons(stoi(_p.second, &_))));
    }

    nfaulty = (replicas.size() - 1) / 3;
    HOTSTUFF_LOG_INFO("nfaulty = %zu", nfaulty);
    connect_all();
    while (try_send());
    ec.dispatch();

// needed for post processing
    for (const auto &e: elapsed)
    {
        char fmt[64];
        struct tm *tmp = localtime(&e.first.tv_sec);
        strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff info] %%.6f\n", tmp);
        fprintf(stderr, fmt, e.first.tv_usec, e.second);
    }
    return 0;
}
