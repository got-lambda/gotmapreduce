(ns gotmapreduce.client
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]))

(defn do-job [ch _ ^bytes payload]
  (let [{:keys [data mapper reducer
                job-id
                total-jobs]}
        (read-string (String. payload "UTF-8"))
        mapper-f (eval mapper)
        reducer-f (eval reducer)
        res (-> data mapper-f reducer-f)]
    (println data res)
    (lb/publish ch "" "gotmapreduce.job-result-queue"
                  (prn-str {:job-id job-id
                            :reducer reducer
                            :total-jobs total-jobs
                            :result res})
                  :content-type "text/plain")))

(defn -main
  []
  (let [con (rmq/connect)
        ch  (lch/open con)]
    (lc/subscribe ch "gotmapreduce.job-queue" do-job
                  :auto-ack true)))


