(ns zerofs-ha.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.util :as util]
            [jepsen.nemesis :as nemesis]
            [slingshot.slingshot :refer [try+]]
            [zerofs-ha.core :as core])
  (:import [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels ServerSocketChannel]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- check [history]
  (checker/check (core/set-checker) {} history {}))

(deftest mount-client-config-defaults-to-fuse
  (is (= "fuse" (:mount-client (core/cfg {:work-dir "/tmp/zerofs-ha"}))))
  (is (= "native" (:mount-client
                    (core/cfg {:work-dir "/tmp/zerofs-ha"
                               :mount-client "native"})))))

(deftest mount-dispatches-to-the-selected-client
  (let [events (atom [])
        awaits (atom 0)
        base   {:work "/tmp/zerofs-ha"
                :zerofs "/tmp/zerofs"
                :mount "/tmp/zerofs-ha/mnt"
                :nodes {:a {:ninep "/tmp/a.sock"}
                        :b {:ninep "/tmp/b.sock"}}}]
    (with-redefs [core/daemon-start!
                  (fn [& args] (swap! events conj [:daemon args]))
                  core/sh-ok!
                  (fn [& args] (swap! events conj [:shell args]) {:exit 0})
                  core/mounted? (constantly true)
                  util/await-fn
                  (fn [ready? _]
                    (swap! awaits inc)
                    (is (true? (ready?))))]
      (core/mount! (assoc base :mount-client "fuse"))
      (core/mount! (assoc base :mount-client "native")))
    (is (= 2 @awaits))
    (is (= [[:daemon
             ["/tmp/zerofs-ha/mount.pid"
              "/tmp/zerofs-ha/mount.log"
              {}
              "/tmp/zerofs"
              ["mount"
               "unix:/tmp/a.sock,unix:/tmp/b.sock"
               "/tmp/zerofs-ha/mnt"
               "--writeback"
               "false"]]]
            [:shell
             [:sudo
              :mount
              :-t
              :zerofs
              :-o
              "consistency=strict,msize=10485760"
              "unix:/tmp/a.sock,unix:/tmp/b.sock"
              "/tmp/zerofs-ha/mnt"]]]
           @events))))

(deftest durability-handles-close-once
  (let [closed  (atom [])
        resource (fn [name]
                   (proxy [java.io.Closeable] []
                     (close [] (swap! closed conj name))))
        handles (atom {"file" {:raf (resource :file)}
                       "dir"  {:dirchan (resource :dir)}})]
    (core/close-durability-handles! handles)
    (core/close-durability-handles! handles)
    (is (= {} @handles))
    (is (= #{:file :dir} (set @closed)))
    (is (= 2 (count @closed)))))

(deftest durability-worker-replacement-preserves-shared-handles
  (let [closed (atom [])
        resource (fn [name]
                   (proxy [java.io.Closeable] []
                     (close [] (swap! closed conj name))))
        entries {"file" {:raf (resource :file) :file "file"}
                 "dir" {:dirchan (resource :dir) :dirname "dir"}}
        handles (atom entries)
        original (core/->DurabilityClient "/unused" handles (atom 0))
        worker (client/open! original {} "n1")]
    ;; This is the close/open sequence used by Jepsen 0.3.7's ClientWorker
    ;; when a crashed process is replaced, before the replacement's next op.
    (client/close! worker {})
    (let [replacement (client/open! original {} "n1")]
      (is (= entries @handles))
      (is (empty? @closed))
      (client/close! replacement {})
      (is (= entries @handles))
      (client/teardown! original {})
      (client/teardown! replacement {})
      (is (empty? @handles))
      (is (= #{:file :dir} (set @closed)))
      (is (= 2 (count @closed))))))

(deftest durability-final-read-survives-worker-replacement
  (let [dir (.toFile (Files/createTempDirectory
                      "zerofs-ha-replacement" (make-array FileAttribute 0)))
        handles (atom {})
        original (core/->DurabilityClient (.getPath dir) handles (atom 0))
        worker (client/open! original {} "n1")]
    (try
      (client/invoke! worker {} {:f :open :file "h"})
      (let [write (client/invoke! worker {} {:f :write :file "h"})
            fsync (client/invoke! worker {} {:f :fsync :file "h"})]
        (is (= :ok (:type write)))
        (is (= :ok (:type fsync)))
        (client/close! worker {})
        (let [replacement (client/open! original {} "n1")
              read (client/invoke! replacement {} {:f :read})
              history [write {:type :invoke :f :fsync :file "h" :process 0}
                       (assoc fsync :process 0) read]
              check (fn [h] (checker/check (core/fsync-honesty-checker) {} h {}))]
          (is (= #{(:value write)} (get-in read [:value "h" :values])))
          (is (:valid? (check history)))
          ;; Keeping the inventory must not weaken the lost-write oracle.
          (is (false? (:valid? (check (assoc history 3 (assoc read :value {}))))))))
      (finally
        (client/teardown! original {})
        (.delete (io/file dir "h"))
        (.delete dir)))))

(deftest final-durability-read-does-not-depend-on-live-handles
  (let [dir      (.toFile (Files/createTempDirectory
                           "zerofs-ha-final-read" (make-array FileAttribute 0)))
        data     (io/file dir "h")
        child    (io/file dir "directory-not-a-data-file")
        handles  (atom {})
        dc       (core/->DurabilityClient (.getPath dir) handles (atom 0))
        history  [{:type :ok :process 0 :f :write :file "h" :value 1533}
                  {:type :invoke :process 0 :f :fsync :file "h"}
                  {:type :ok :process 0 :f :fsync :file "h"}]
        checked? (fn [op] (:valid? (checker/check
                                    (core/fsync-honesty-checker)
                                    {} (conj history op) {})))]
    (try
      (spit data "1533\n")
      (is (.mkdir child))
      (reset! handles {"h" {:raf (java.io.RandomAccessFile. data "rw")
                            :file "h"}})
      (core/close-durability-handles! handles)
      (is (empty? @handles))
      ;; An acknowledged fsync must remain verifiable even after a Jepsen
      ;; worker has closed the shared descriptor registry.
      (let [observed (client/invoke! dc {} {:f :read})]
        (is (= #{"h"} (set (keys (:value observed)))))
        (is (= {:size 5 :values (sorted-set 1533)}
               (get (:value observed) "h")))
        (is (true? (checked? observed))))
      ;; Deleting the actual file must still be detected as a lost fsync.
      (is (.delete data))
      (let [missing (client/invoke! dc {} {:f :read})]
        (is (= {} (:value missing)))
        (is (false? (checked? missing))))
      (finally
        (.delete data)
        (.delete child)
        (.delete dir)))))

(deftest unmount-dispatches-to-the-selected-client
  (let [commands (atom [])
        base     {:mount "/tmp/zerofs-ha/mnt"}]
    (with-redefs [core/mounted? (constantly true)
                  core/sh! (fn [& args]
                             (swap! commands conj args)
                             {:exit 0})
                  core/sh-ok! (fn [& args]
                                (swap! commands conj args)
                                {:exit 0})]
      (core/unmount! (assoc base :mount-client "fuse"))
      (core/unmount! (assoc base :mount-client "native")))
    (is (= [[:fusermount3 :-uz "/tmp/zerofs-ha/mnt"]
            [:sudo :umount "/tmp/zerofs-ha/mnt"]]
           @commands))))

(deftest node-9p-up-requires-a-listener
  (let [dir         (.toFile (Files/createTempDirectory
                              "zerofs-ha-test"
                              (make-array FileAttribute 0)))
        socket      (io/file dir "ninep.sock")
        socket-path (.getPath socket)
        c           {:nodes {:a {:ninep socket-path}}}]
    (try
      (spit socket "")
      (is (false? (core/node-9p-up? c :a)))
      (is (.delete socket))
      (with-open [server (ServerSocketChannel/open StandardProtocolFamily/UNIX)]
        (.bind server (UnixDomainSocketAddress/of socket-path))
        (is (true? (core/node-9p-up? c :a))))
      (finally
        (.delete socket)
        (.delete dir)))))

(deftest standby-ready-count-requires-an-accepted-heartbeat
  (let [log-a (java.io.File/createTempFile "zerofs-ha-a" ".log")
        log-b (java.io.File/createTempFile "zerofs-ha-b" ".log")
        armed "HA standby: accepted leader heartbeat; takeover watch armed"
        c {:nodes {:a {:log (.getPath log-a)}
                   :b {:log (.getPath log-b)}}}]
    (try
      ;; Starting the watcher alone must not make Jepsen kill the leader.
      (spit log-a (str "HA standby a: watching leader heartbeats\n"
                       armed "\n"))
      (spit log-b "HA standby b: watching leader heartbeats\n")
      (is (= 1 (core/standby-ready-count c :a)))
      (is (zero? (core/standby-ready-count c :b)))
      ;; Count only events from this node, emitted after covered liveness arms.
      (spit log-b (str armed "\n" armed "\n") :append true)
      (is (= 2 (core/standby-ready-count c :b)))
      (is (= 1 (core/standby-ready-count c :a)))
      (finally
        (.delete log-a)
        (.delete log-b)))))

(deftest await-serving-requires-promoted-node
  (let [previous @core/cluster-roles
        probe (fn [roles op live]
                (reset! core/cluster-roles roles)
                (let [seen (atom [])
                      accepted (atom nil)]
                  (with-redefs [core/node-9p-up?
                                (fn [_ node]
                                  (swap! seen conj node)
                                  (contains? live node))
                                util/await-fn
                                (fn [predicate _]
                                  (reset! accepted
                                          (try (boolean (predicate))
                                               (catch Throwable _ false))))]
                    (nemesis/invoke! (core/ha-nemesis)
                                     {:work-dir "/tmp/zerofs-ha-test"} op))
                  [@accepted @seen]))]
    (try
      ;; SIGKILL need not close the old socket before the next poll.
      (is (= [false [:b]]
             (probe {:a :dead :b :leader} {:f :await-serving} #{:a})))
      (is (= [true [:b]]
             (probe {:a :dead :b :leader} {:f :await-serving} #{:b})))
      ;; The Solo leader still accepts connections during partition.
      (is (= [false [:b]]
             (probe {:a :leader :b :standby}
                    {:f :await-serving :target :standby} #{:a})))
      (is (= [true [:b]]
             (probe {:a :leader :b :standby}
                    {:f :await-serving :target :standby} #{:b})))
      (finally (reset! core/cluster-roles previous)))))

(deftest make-bucket-waits-for-a-readable-bucket
  (let [commands (atom [])
        stats    (atom 0)
        c        {:mc "/mc" :minio-addr "127.0.0.1:9000"
                  :access-key "key" :secret-key "secret" :bucket "bucket"}]
    (with-redefs [core/sh-ok!
                  (fn [& args]
                    (swap! commands conj args)
                    (when (and (= "stat" (second args))
                               (= 1 (swap! stats inc)))
                      (throw (ex-info "bucket not ready" {})))
                    {:exit 0})
                  util/await-fn
                  (fn [ready? _]
                    (try
                      (ready?)
                      (catch clojure.lang.ExceptionInfo _
                        (ready?))))]
      (core/make-bucket! c)
      (is (= ["alias" "mb" "stat" "alias" "mb" "stat"]
             (mapv second @commands))))))

(deftest heal-restart-starts-the-standby-before-waiting-for-the-leader
  (let [events        (atom [])
        standby-ready (atom 4)]
    (with-redefs [core/cluster-roles (atom {})
                  core/minio-up? (constantly true)
                  core/node-pid (fn [_ node-key] node-key)
                  core/kill-pid! (fn [_])
                  core/standby-ready-count
                  (fn [_ node-key]
                    (swap! events conj [:standby-count node-key @standby-ready])
                    @standby-ready)
                  core/start-node!
                  (fn [_ node-key role]
                    (swap! events conj [:start node-key role]))
                  core/node-9p-up? (fn [_ _] true)
                  core/mounted? (fn [_] true)
                  util/await-fn
                  (fn [ready? options]
                    (let [message (:log-message options)]
                      (swap! events conj [:await message])
                      (case message
                        "heal: waiting for leader" (is (true? (ready?)))
                        "Waiting for standby b" (do
                                                   (is (thrown? clojure.lang.ExceptionInfo
                                                                (ready?)))
                                                   (swap! standby-ready inc)
                                                   (is (true? (ready?))))
                        "heal: waiting for mount" (is (true? (ready?))))))]
      (core/heal-restart! {})
      (is (= [[:standby-count :b 4]
              [:start :b "standby"]
              [:start :a "leader"]
              [:await "heal: waiting for leader"]
              [:await "Waiting for standby b"]
              [:standby-count :b 4]
              [:standby-count :b 5]
              [:await "heal: waiting for mount"]]
             @events))
      (is (= {:a :leader :b :standby} @core/cluster-roles)))))

(deftest set-checker-flags-lost-and-resurrected
  (testing "catches a lost add (present per ops, missing from the final read) and a
            resurrected remove (absent per ops, present in the final read), while
            ignoring indeterminate (:info) values"
    (let [r (check [{:type :ok   :f :add    :value 1}        ; present, will be read -> ok
                    {:type :ok   :f :add    :value 2}        ; added then removed -> absent, ok
                    {:type :ok   :f :remove :value 2}
                    {:type :ok   :f :add    :value 3}        ; present per ops but NOT read -> LOST
                    {:type :ok   :f :add    :value 4}        ; removed but present in read -> RESURRECTED
                    {:type :ok   :f :remove :value 4}
                    {:type :info :f :add    :value 5}        ; indeterminate -> skipped either way
                    {:type :fail :f :remove}                 ; no :value -> ignored
                    {:type :ok   :f :read   :value (sorted-set 1 4 5)}])]
      (is (false? (:valid? r)))
      (is (= [3] (:lost r)))
      (is (= [4] (:resurrected r))))))

(deftest set-checker-passes-a-consistent-history
  (testing "a history where every present/absent value matches the final read is valid"
    (let [r (check [{:type :ok   :f :add    :value 1}        ; present + read
                    {:type :ok   :f :add    :value 2}        ; removed + not read
                    {:type :ok   :f :remove :value 2}
                    {:type :info :f :add    :value 3}        ; indeterminate, happens to be read
                    {:type :ok   :f :read   :value (sorted-set 1 3)}])]
      (is (true? (:valid? r)))
      (is (zero? (:lost-count r)))
      (is (zero? (:resurrected-count r))))))

(deftest set-checker-info-after-ok-is-indeterminate
  (testing "an :info op after an :ok add makes the value indeterminate (not asserted)"
    (let [r (check [{:type :ok   :f :add    :value 1}
                    {:type :info :f :remove :value 1}        ; might have removed -> indeterminate
                    {:type :ok   :f :read   :value (sorted-set)}])]  ; absent, but that's allowed
      (is (true? (:valid? r))))))

(deftest set-checker-bounds-anomaly-samples
  (let [lost-values        (range 60)
        resurrected-values (range 100 160)
        history            (concat
                            (map (fn [v] {:type :ok :f :add :value v}) lost-values)
                            (mapcat (fn [v] [{:type :ok :f :add :value v}
                                             {:type :ok :f :remove :value v}])
                                    resurrected-values)
                            [{:type :ok :f :read :value (into (sorted-set)
                                                              resurrected-values)}])
        r                  (check history)]
    (is (false? (:valid? r)))
    (is (= 60 (:lost-count r)))
    (is (= (vec (range 50)) (:lost r)))
    (is (= 60 (:resurrected-count r)))
    (is (= (vec (range 100 150)) (:resurrected r)))))

(deftest add-error-classification
  (testing "ENOENT/EEXIST from the rename are indeterminate: a resent rename whose
            original applied re-executes exactly this way. The NIO two-path
            exceptions carry a null reason (message is just \"src -> dst\", no
            errno text), so classification must not depend on the message"
    (is (= {:type :info :error :not-durable-here}
           (core/classify-add-error
            (java.nio.file.NoSuchFileException. "/mnt/d1/.tmp-9" "/mnt/d1/9" nil))))
    (is (= {:type :info :error :retried-create}
           (core/classify-add-error
            (java.nio.file.FileAlreadyExistsException. "/mnt/d1/.tmp-9" "/mnt/d1/9" nil)))))
  (testing "single-path java.io exceptions still classify by message text"
    (is (= {:type :info :error :not-durable-here}
           (core/classify-add-error
            (java.io.FileNotFoundException. "/mnt/d1/9 (No such file or directory)"))))
    (is (= {:type :info :error :retried-create}
           (core/classify-add-error
            (java.io.IOException. "File exists")))))
  (testing "other IO errors are indeterminate after a multi-step add"
    (is (= :info (:type (core/classify-add-error
                         (java.io.IOException. "Bad file descriptor")))))
    (is (= :info (:type (core/classify-add-error
                         (java.io.IOException. "Input/output error")))))))

(deftest inode-accounting-waits-for-replay-protected-orphans
  (let [samples (atom [20 19])
        options (atom nil)]
    (with-redefs [core/count-all-inodes (constantly 20)
                  core/statfs-used-inodes
                  (fn [_]
                    (let [sample (first @samples)]
                      (swap! samples rest)
                      sample))
                  util/await-fn
                  (fn [f opts]
                    (reset! options opts)
                    (try+
                      (f)
                      (catch [:type ::core/inode-reclamation-pending] pending
                        (is (= 20 (:used-inodes pending)))
                        (is (= 19 (:expected-used-inodes pending)))
                        (f))))]
      (is (= 19 (core/await-inode-accounting!
                 "/mnt" {:used-inodes 17 :all-inodes 18})))
      (is (empty? @samples))
      (is (= core/inode-accounting-timeout-ms (:timeout @options)))
      (is (> (:timeout @options) 150000)))))

(deftest statfs-checker-retains-the-orphan-leak-check
  (let [history [{:type :ok :f :read :value #{}
                  :used-inodes 17 :all-inodes 18}
                 {:type :ok :f :read :value #{1 2}
                  :used-inodes 20 :all-inodes 20}]
        result (checker/check (core/statfs-checker) {} history {})]
    (is (false? (:valid? result)))
    (is (= 3 (:inodes-grew result)))
    (is (= 2 (:census-grew result)))))

