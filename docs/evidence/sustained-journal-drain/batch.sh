#!/usr/bin/env bash
# Sequential measurement batch on Studio Docker (native linux/arm64, ext4 volume).
H=/Volumes/bigssd/projects/ZeroFS-targets/zsd-a24001
W=/Users/zackjackson/.t3/worktrees/ZeroFS/perf-sustained-journal--fbd3e751398982de
cd "$W"
export SCRATCH_VOLUME=zsd-a24001-scratch IMAGE=zsd-a24001-run:local
B=$H/bin/base-hwsha-63ad64ce
N=$H/bin/head-hwsha-55548730
V1=$W/scripts/fixtures/nas-journal/engine.toml.in
V2=$H/templates/v2-j16384m-u8-vol.toml.in
V2S=$H/templates/v2-j4096m-u8-vol.toml.in
run() { # label binary template files mib args...
  local label=$1; shift
  bash scripts/run-nas-journal-sustained.sh "$H" "$@" > /dev/null 2>&1
  echo "$(date +%T) $label exit $(cat $H/evidence/$label/exit.txt | head -1)" >> $H/logs/batch.log
}
run m1-base-1 $B $V1 m1-base-1 1 8192 --write-kib 512
run m1-head-1 $N $V1 m1-head-1 1 8192 --write-kib 512
run m1-base-2 $B $V1 m1-base-2 1 8192 --write-kib 512
run m1-head-2 $N $V1 m1-head-2 1 8192 --write-kib 512
run m2-head-1 $N $V2 m2-head-1 1 8192 --write-kib 512
run m2-head-2 $N $V2 m2-head-2 1 8192 --write-kib 512
run m3-base-d1 $B $V1 m3-base-d1 1 2048 --write-kib 512 --depth 1
run m3-head-d1 $N $V2 m3-head-d1 1 2048 --write-kib 512 --depth 1
docker run --rm --name zsd-a24001-ctl-d1 --network none --cap-drop ALL --security-opt no-new-privileges \
  --cpus 8 --memory 8g --memory-swap 8g -v "$W/scripts:/fixtures:ro" -v zsd-a24001-scratch:/scratch \
  -v "$H/evidence:/ev" zsd-a24001-run:local python3 -B /fixtures/bench-plain-copy-control.py \
  --scratch /scratch --evidence /ev/m3-plain-d1 --file-mib 2048 --write-kib 512 --depth 1 --mode buffered > /dev/null 2>&1
echo "$(date +%T) m3-plain-d1 exit $?" >> $H/logs/batch.log
run m4-head-b2b $N $V2S m4-head-b2b 2 8192 --write-kib 512
run m5-head-crash $N $V2S m5-head-crash 1 2048 --write-kib 512 --crash-after-fsync
run m6-head-tail $N $V2S m6-head-tail 1 2048 --write-kib 512 --tail-reader-mib 512
echo "$(date +%T) batch-done" >> $H/logs/batch.log
