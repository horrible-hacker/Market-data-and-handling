#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <chrono>
#include <algorithm>
#include <string>
#include <deque>
#include <mutex>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/lockfree/queue.hpp>
#include <nlohmann/json.hpp>
using namespace std;
namespace asio   = boost::asio;
namespace ssl    = asio::ssl;
namespace beast  = boost::beast;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;
using json = nlohmann::json;
using Clock = chrono::high_resolution_clock;
using us = chrono::microseconds;

static const vector<string> SYMBOLS = {"btcusdt", "ethusdt", "bnbusdt"};
static const int RUN_SECONDS = 20;
static const size_t POOL_SIZE = 1 << 14;
static const size_t CONSUMER_QCAP = 1 << 14;
static const int SAMPLE_RATE = 10;

struct Tick 
{
    long long seq;
    int symbol_id;
    double price;
    long long ts_us;
    atomic<int> refcount;
};

class TickPool 
{
    vector<Tick> storage;
    vector<Tick*> freelist;
    atomic<size_t> top;
public:
    TickPool(size_t cap) : storage(cap), freelist(cap), top(cap) 
    {
        for (size_t i = 0; i < cap; ++i) {
            freelist[i] = &storage[i];
            storage[i].seq = 0;
            storage[i].symbol_id = -1;
            storage[i].price = 0.0;
            storage[i].ts_us = 0;
            storage[i].refcount.store(0);
        }
    }
    Tick* borrow() 
    {
        size_t cur;
        do {
            cur = top.load(memory_order_acquire);
            if (cur == 0) return nullptr;
        } while (!top.compare_exchange_weak(cur, cur - 1, memory_order_acq_rel));
        return freelist[cur - 1];
    }
    void giveBack(Tick* t) 
    {
        t->refcount.store(0, memory_order_relaxed);
        size_t cur;
        do {
            cur = top.load(memory_order_acquire);
        } while (!top.compare_exchange_weak(cur, cur + 1, memory_order_acq_rel));
        freelist[cur] = t;
    }
};
template<typename T>
class SPSCQueue {
    const size_t capacity;
    const size_t mask;
    vector<T> buffer;
    atomic<size_t> head;
    atomic<size_t> tail; 
public:
    SPSCQueue(size_t cap_pow2) : capacity(cap_pow2), mask(cap_pow2 - 1), buffer(cap_pow2) 
    {
        if ((cap_pow2 & (cap_pow2 - 1)) != 0) {
            throw runtime_error("capacity must be power of two");
        }
        head.store(0, memory_order_relaxed);
        tail.store(0, memory_order_relaxed);
    }
    bool push(const T &v) 
    {
        size_t h = head.load(memory_order_relaxed);
        size_t next = (h + 1) & mask;
        size_t t = tail.load(memory_order_acquire);
        if (next == t) return false; // full
        buffer[h] = v;
        head.store(next, memory_order_release);
        return true;
    }
    bool pop(T &out) 
    {
        size_t t = tail.load(memory_order_relaxed);
        size_t h = head.load(memory_order_acquire);
        if (t == h) return false; // empty
        out = buffer[t];
        tail.store((t + 1) & mask, memory_order_release);
        return true;
    }
    bool empty() 
    {
        return tail.load(memory_order_acquire) == head.load(memory_order_acquire);
    }
};
TickPool *g_pool = nullptr;
vector<SPSCQueue<Tick*>*> symbolQueues;
struct ConsumerQueues 
{
    vector<boost::lockfree::queue<Tick*>*> qs;
    void init(size_t n) 
    {
        qs.resize(n);
        for (size_t i = 0; i < n; ++i) qs[i] = new boost::lockfree::queue<Tick*>(CONSUMER_QCAP);
    }
    ~ConsumerQueues() 
    {
        for (auto q : qs) delete q;
    }
} consumerQs;
atomic<bool> running{true};
vector<long long> latency_samples;
mutex latency_mtx;
atomic<size_t> sample_counter{0};
static inline long long now_us() 
{
    return chrono::duration_cast<us>(Clock::now().time_since_epoch()).count();
}
static inline double parse_price(const json &j, const string &fld="p") 
{
    try 
    {
        if (j[fld].is_string()) return stod(j[fld].get<string>());
        if (j[fld].is_number()) return j[fld].get<double>();
    } 
    catch (...) {}
    return 0.0;
}
void symbol_producer(const string &symbol, int symbol_id, TickPool &pool, SPSCQueue<Tick*> &outq) 
{
    const string host = "stream.binance.com";
    const string port = "9443";
    const string target = "/ws/" + symbol + "@trade";
    try 
    {
        asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();
        ctx.set_verify_mode(ssl::verify_peer);
        websocket::stream<ssl::stream<tcp::socket>> ws(ioc, ctx);
        tcp::resolver resolver(ioc);
        auto const results = resolver.resolve(host, port);
        asio::connect(ws.next_layer().next_layer(), results.begin(), results.end());
        ws.next_layer().handshake(ssl::stream_base::client);
        ws.handshake(host, target);
        long long seq = 0;
        while (running.load(memory_order_relaxed)) 
        {
            beast::flat_buffer buffer;
            ws.read(buffer); // blocking
            string msg = beast::buffers_to_string(buffer.data());
            json j;
            try { j = json::parse(msg); }
            catch (...) { continue; }
            double price = parse_price(j, "p");
            Tick* t = nullptr;
            while ((t = pool.borrow()) == nullptr) 
            {
                if (!running.load()) break;
                this_thread::sleep_for(chrono::microseconds(10));
            }
            if (!t) break;
            t->seq = ++seq;
            t->symbol_id = symbol_id;
            t->price = price;
            t->ts_us = now_us();
            t->refcount.store(0);
            while (!outq.push(t)) 
            {
                if (!running.load()) { pool.giveBack(t); break; }
                this_thread::sleep_for(chrono::microseconds(1));
            }
        }
        beast::error_code ec;
        ws.close(websocket::close_code::normal, ec);
        (void)ec;
    } 
    catch (const std::exception &e) 
    {
        cerr << "[producer " << symbol << "] exception: " << e.what() << "\n";
    }
}
void dispatcher_worker(TickPool &pool, int num_consumers) 
{
    const size_t nqs = symbolQueues.size();
    size_t idx = 0;
    while (running.load(memory_order_relaxed)) 
    {
        bool any = false;
        for (size_t i = 0; i < nqs; ++i) 
        {
            idx = (idx + 1) % nqs;
            Tick* t = nullptr;
            if (symbolQueues[idx]->pop(t)) 
            {
                any = true;
                t->refcount.store(num_consumers, memory_order_release);
                for (int c = 0; c < num_consumers; ++c) 
                {
                    auto q = consumerQs.qs[c];
                    bool pushed = false;
                    for (int a = 0; a < 100; ++a) 
                    {
                        if (q->push(t)) { pushed = true; break; }
                        this_thread::sleep_for(chrono::microseconds(1));
                    }
                    if (!pushed) 
                    {
                        int prev = t->refcount.fetch_sub(1, memory_order_acq_rel);
                        if (prev == 1) pool.giveBack(t);
                    }
                }
            }
        }
        if (!any) 
        {
            this_thread::sleep_for(chrono::microseconds(2));
        }
    }
    for (auto q : symbolQueues) 
    {
        Tick* t = nullptr;
        while (q->pop(t)) 
        {
            if (!t) break;
            t->refcount.store(num_consumers, memory_order_release);
            for (int c=0;c<num_consumers;++c) {
                auto cq = consumerQs.qs[c];
                if (!cq->push(t)) 
                {
                    int prev = t->refcount.fetch_sub(1, memory_order_acq_rel);
                    if (prev == 1) pool.giveBack(t);
                }
            }
        }
    }
}
void latency_consumer(TickPool &pool, int consumer_idx) 
{
    auto q = consumerQs.qs[consumer_idx];
    while (running.load(memory_order_relaxed)) 
    {
        Tick* t = nullptr;
        if (q->pop(t)) 
        {
            if (!t) continue;
            size_t c = sample_counter.fetch_add(1, memory_order_relaxed);
            if ((c % SAMPLE_RATE) == 0) 
            {
                long long lat = now_us() - t->ts_us;
                lock_guard<mutex> lk(latency_mtx);
                latency_samples.push_back(lat);
            }
            int prev = t->refcount.fetch_sub(1, memory_order_acq_rel);
            if (prev == 1) pool.giveBack(t);
        } 
        else 
        {
            this_thread::sleep_for(chrono::microseconds(1));
        }
    }
    Tick* t = nullptr;
    while (q->pop(t)) 
    {
        if (!t) break;
        size_t c = sample_counter.fetch_add(1, memory_order_relaxed);
        if ((c % SAMPLE_RATE) == 0) 
        {
            long long lat = now_us() - t->ts_us;
            lock_guard<mutex> lk(latency_mtx);
            latency_samples.push_back(lat);
        }
        int prev = t->refcount.fetch_sub(1, memory_order_acq_rel);
        if (prev == 1) pool.giveBack(t);
    }
}
void strategy_consumer(TickPool &pool, int consumer_idx) 
{
    auto q = consumerQs.qs[consumer_idx];
    const int WINDOW = 8;
    vector<deque<double>> windows(SYMBOLS.size());
    while (running.load(memory_order_relaxed)) {
        Tick* t = nullptr;
        if (q->pop(t)) 
        {
            if (!t) continue;
            int sid = t->symbol_id;
            double price = t->price;
            auto &w = windows[sid];
            w.push_back(price);
            if ((int)w.size() > WINDOW) w.pop_front();
            if ((int)w.size() == WINDOW) 
            {
                double s = 0;
                for (double p: w) s += p;
                double ma = s / w.size();
                if (price > ma * 1.0005) 
                {
                    cout << "[STRAT] " << SYMBOLS[sid] << " price=" << price << " ma=" << ma << " BUY\n";
                }
            }
            int prev = t->refcount.fetch_sub(1, memory_order_acq_rel);
            if (prev == 1) pool.giveBack(t);
        } 
        else 
        {
            this_thread::sleep_for(chrono::microseconds(1));
        }
    }
    Tick* t = nullptr;
    while (q->pop(t)) 
    {
        if (!t) break;
        int sid = t->symbol_id;
        double price = t->price;
        auto &w = windows[sid];
        w.push_back(price);
        if ((int)w.size() > WINDOW) w.pop_front();
        int prev = t->refcount.fetch_sub(1, memory_order_acq_rel);
        if (prev == 1) pool.giveBack(t);
    }
}
void logger_consumer(TickPool &pool, int consumer_idx) 
{
    auto q = consumerQs.qs[consumer_idx];
    while (running.load(memory_order_relaxed)) 
    {
        Tick* t = nullptr;
        if (q->pop(t)) 
        {
            if (!t) continue;
            cout << "[LOG] " << SYMBOLS[t->symbol_id] << " seq=" << t->seq << " price=" << t->price << "\n";
            int prev = t->refcount.fetch_sub(1, memory_order_acq_rel);
            if (prev == 1) pool.giveBack(t);
        } 
        else 
        {
            this_thread::sleep_for(chrono::microseconds(1));
        }
    }
    Tick* t = nullptr;
    while (q->pop(t)) 
    {
        if (!t) break;
        cout << "[LOG] " << SYMBOLS[t->symbol_id] << " seq=" << t->seq << " price=" << t->price << "\n";
        int prev = t->refcount.fetch_sub(1, memory_order_acq_rel);
        if (prev == 1) pool.giveBack(t);
    }
}
void print_latency_stats() 
{
    lock_guard<mutex> lk(latency_mtx);
    if (latency_samples.empty()) 
    {
        cout << "No latency samples collected.\n";
        return;
    }
    auto copy = latency_samples;
    sort(copy.begin(), copy.end());
    size_t n = copy.size();
    auto p50 = copy[n * 50 / 100];
    auto p95 = copy[min(n-1, n * 95 / 100)];
    auto p99 = copy[min(n-1, n * 99 / 100)];
    long long sum = 0;
    for (auto v : copy) sum += v;
    double avg = double(sum) / double(n);
    cout << "\nLatency samples: " << n << "\n";
    cout << "Avg: " << avg << " us (" << avg/1000.0 << " ms)\n";
    cout << "p50: " << p50 << " us\n";
    cout << "p95: " << p95 << " us\n";
    cout << "p99: " << p99 << " us\n";
}
int main() 
{
    ios::sync_with_stdio(false);
    cin.tie(nullptr);
    TickPool pool(POOL_SIZE);
    g_pool = &pool;
    size_t spsc_cap = 1 << 12; 
    for (size_t i = 0; i < SYMBOLS.size(); ++i) 
    {
        symbolQueues.push_back(new SPSCQueue<Tick*>(spsc_cap));
    }
    const int NUM_CONSUMERS = 3;
    consumerQs.init(NUM_CONSUMERS);
    thread dispatcher(dispatcher_worker, ref(pool), NUM_CONSUMERS);
    thread latThread(latency_consumer, ref(pool), 0);
    thread stratThread(strategy_consumer, ref(pool), 1);
    thread logThread(logger_consumer, ref(pool), 2);
    vector<thread> producers;
    for (size_t i = 0; i < SYMBOLS.size(); ++i) 
    {
        producers.emplace_back(symbol_producer, SYMBOLS[i], (int)i, ref(pool), ref(*symbolQueues[i]));
    }
    cout << "Running for " << RUN_SECONDS << " seconds...\n";
    this_thread::sleep_for(chrono::seconds(RUN_SECONDS));
    running.store(false);
    for (auto &t : producers) if (t.joinable()) t.join();
    this_thread::sleep_for(chrono::milliseconds(100));
    if (dispatcher.joinable()) dispatcher.join();
    if (latThread.joinable()) latThread.join();
    if (stratThread.joinable()) stratThread.join();
    if (logThread.joinable()) logThread.join();
    print_latency_stats();
    for (auto q : symbolQueues) delete q;
    cout << "Exiting.\n";
    return 0;
}
