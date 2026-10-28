OUTDIR="./out"
# TMPDIR="./tmp/"
WRKDIR="./workDir"
JOBSTORE="./jobstore"
LOGDIR="./log"
DATADIR="/var/scratch/kdirker"

if [ ! -f "$DATADIR/image_mf_01.fits" ] || [ ! -f "$DATADIR/image_mf_02.fits" ] || [ ! -f "$DATADIR/image_mf_04.fits" ]; then
    echo "Missing input fit(s)"
    exit
fi

if test -d $OUTDIR; then rm -rf $OUTDIR; fi
if test -d $TMPDIR; then rm -rf $TMPDIR; fi
if test -d $WRKDIR; then rm -rf $WRKDIR; fi
if test -d $JOBSTORE; then rm -rf $JOBSTORE; fi
if test -d $LOGDIR; then rm -rf $LOGDIR; fi

# mkdir $OUTDIR $TMPDIR $WRKDIR $LOGDIR


source env/bin/activate
toil-cwl-runner \
    --batchSystem slurm \
    --slurmAllocateMem False \
    --defaultCores 10 \
    --defaultWalltime 900 \
    --outdir $OUTDIR \
    --jobStore $JOBSTORE \
    --clean always \
    --logFile toil_runtime.log \
    --writeLogs $LOGDIR \
    --batchLogsDir $LOGDIR \
    --stats \
    --bypass-file-store \
    process_images.cwl \
    process_images.yaml
    # --workDir $WRKDIR \
    # --logDebug \
    # --writeLogsFromAllJobs \
    # --tmpdir-prefix $TMPDIR \
    # --leave-tmpdir \
    # --retryCount 0 \
