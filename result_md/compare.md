實驗觀察:
---

1. 同樣配置, 並行3個topology, 寫入/不寫入mongodb, 於spout consume & emit 420000筆資料的花費時間分別為70 / 80 sec -> 表示寫入影響到讀取速度  

2. 相同配置, 執行1個topology / 並行3個topology, 不寫入mongodb, 於spout consume & emit 420000筆資料的花費時間分別為70 / 34 sec -> 表示三倍的topology會彼此影響  

3. 相同配置, 執行1個topology / 並行3個topology, 寫入mongodb, 於spout consume & emit 420000筆資料的花費時間分別為 / 39 sec -> 表示三倍的topology會彼此影響  

3. 相同配置, 執行1個topology, 寫入/不寫入mongodb, 於spout consume & emit 420000筆資料的花費時間分別為39 / 34 sec -> 表示寫入影響到讀取速度, 可能是IO或bolt處理速度不足影響spout  

4. 單元測試3個 kafka client同時分別consume 3個topic的message, consume時間與1個client相比, consume 420000筆資料時間從17.2秒 變為 20.8秒  
    -> 與項目2比對, 項目2花費時間不應為兩倍; 推論 3個topology同時運行(去掉寫入)時遇到的瓶頸應該不是IO, 可能是cpu (運行時3*6個core使用率在75%~100%)  
    
5. 並行3個topology時以切分問題/資料時, 如果不每個topology使用3個worker, 只指定1個的話, 執行速度反而高於開3個worker (起較少的JVM)  

6. 根據項目4與5, 總硬體效能不足時, 硬提高工作的分散程度, 增加的基本開銷有可能反而降低整體效能  

7. storm.yaml調整topology.worker.shared.thread.pool.size, 沒有明顯效能改變  

8. 在1 worker情況, 把output bolt調得更高, 寫入的時間仍沒有加速效果, 但執行後段(consume/split/group已完成)的cpu使用率有提高  

9. 使用1 topology 3 worker 總task數相近的情況, cpu不會全滿  