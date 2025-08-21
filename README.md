# Real-Time Trading Data Pipeline with Lock-Free Queues
- Designed and implemented a **low-latency multi-symbol market data pipeline** in C++ using **Boost lock-free queues**, capable of processing thousands of Binance WebSocket messages per second with sub-millisecond latency.  
- Built a **multi-consumer architecture** to run trading strategies in parallel, with latency benchmarking (p50: 20µs, p99: 65µs) ensuring deterministic performance for real-time decision-making.  
# Output
[!Output photo]
