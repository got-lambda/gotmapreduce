(ns gotmapreduce.core
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]))

(def db (atom {}))

(defmacro defjob [name & code]
  `(def ~name
     (quote ~(cons 'fn code))))

(defjob test-map [nums]
  (map inc nums))

(defjob test-reduce [nums]
  (reduce + nums))

(def log (atom []))

(defn handle-resp
  [ch _ ^bytes payload]
  (let [{:keys [job-id result total-jobs reducer]}
        (read-string (String. payload "UTF-8"))
        reducer (eval reducer)]
    (swap! log conj (str "GOT SOME RESP"
                         [job-id result total-jobs]))
    (let [state (swap! db update-in [job-id] conj result)]
      (when (= total-jobs (count (state job-id)))
        (let [res (reducer (state job-id))]
          (swap! log conj (str "job done:" job-id "\nresult:"
                               res)))))))

(defn start-job [channel
                 data
                 mapper
                 reducer]  
  (let [job-id (java.util.UUID/randomUUID)
        jobs (partition 5 data)]
    (doseq [[index job] (map vector (range) jobs)]
      (println "sending job"
               job-id
               index job)
      (lb/publish channel "" "gotmapreduce.job-queue"
                  (prn-str {:job-id job-id
                            :total-jobs (count jobs)
                            :id index 
                            :data job
                            :mapper mapper
                            :reducer reducer})
                  :content-type "text/plain"
                  :type "gotmapreduce.job"))))

(defn init-queue
  []
  (let [con (rmq/connect)
        chan (lch/open con)
        chan-name "gotmapreduce.job-queue"]
    (lq/declare chan "gotmapreduce.job-queue"
                :exclusive false
                :auto-delete false)
    (lq/declare chan "gotmapreduce.job-result-queue"
                :exclusive false
                :auto-delete false)
    (future
      (lc/subscribe chan "gotmapreduce.job-result-queue"
                    handle-resp
                    :auto-ack true))
    {:con con
     :chan chan}))


(def con (init-queue))

(start-job (:chan con)
           (range 100)
           test-map
 test-reduce)


