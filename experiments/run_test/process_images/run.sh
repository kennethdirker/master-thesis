OUTDIR="./output_offline"
TMPDIR="./tmp_offline"
DATADIR="/var/scratch/kdirker"

if [ ! -f "$DATADIR/image_mf_01.fits" ] || [ ! -f "$DATADIR/image_mf_02.fits" ] || [ ! -f "$DATADIR/image_mf_04.fits" ]; then
    echo "Missing input fit(s)"
    exit
fi

if test -d $OUTDIR; then rm -rf $OUTDIR; fi
if test -d $TMPDIR; then rm -rf $TMPDIR; fi

source env/bin/activate
# python3 process_images.py --outdir $OUTDIR --tmpdir $TMPDIR --leave_tmp process_images_sub.yaml
python3 process_images_sub.py --outdir $OUTDIR --tmpdir $TMPDIR --leave_tmp process_images_sub.yaml