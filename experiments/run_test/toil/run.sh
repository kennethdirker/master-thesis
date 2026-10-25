OUTDIR="./out"
TMPDIR="./tmp"
LOGDIR="./log"
DATADIR="/var/scratch/kdirker"

if [ ! -f "$DATADIR/image_mf_01.fits" ] || [ ! -f "$DATADIR/image_mf_02.fits" ] || [ ! -f "$DATADIR/image_mf_04.fits" ]; then
    echo "Missing input fit(s)"
    exit
fi

if test -d $OUTDIR; then rm -rf $OUTDIR; fi
if test -d $TMPDIR; then rm -rf $TMPDIR; fi

source env/bin/activate
toil-cwl-runner \
    # --jobStore my_jobStore \
    --batchSystem slurm \
    --workDir $TMPDIR \
    --outdir $OUTDIR \
    --logFile cwltoil.log \
    --writeLogs $LOGDIR \
    --retryCount 0 \
    --maxLogFileSize 20000000000 \
    --stats \
    process_images.cwl \
    process_images-job.yaml