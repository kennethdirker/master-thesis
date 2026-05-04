OUT=out.png
NOISE=no_noise_image.fits
NOISEOUT=no_noise_out.png
TMPDIR=/var/scratch/kdirker/tmp_dir
OUTDIR=/var/scratch/kdirker/out_dir

if test -f $OUTDIR/$OUT; then
    rm $OUTDIR/$OUT
fi
if test -f $OUTDIR/$NOISE; then
    rm $OUTDIR/$NOISE
fi
if test -f $OUTDIR/$NOISEOUT; then
    rm $OUTDIR/$NOISEOUT
fi
if test -d $TMPDIR; then
    rm -rf $TMPDIR
fi
if test -d $OUTDIR; then
    rm -rf $OUTDIR
fi

# Execute the tools simulating the workflow in the workflow directory
# python download_images.py download_images.yaml    < Doesn't work because url is broken
python imageplotter.py -y imageplotter.yaml --preserve_tmp --tmpdir $TMPDIR --outdir $OUTDIR --dask --slurm_config ../workflows/slurm.config
python noiseremover.py -y noiseremover.yaml --preserve_tmp --tmpdir $TMPDIR --outdir $OUTDIR --dask --slurm_config ../workflows/slurm.config
python imageplotter.py -y imageplotter_after.yaml --preserve_tmp --tmpdir $TMPDIR --outdir $OUTDIR --dask --slurm_config ../workflows/slurm.config
