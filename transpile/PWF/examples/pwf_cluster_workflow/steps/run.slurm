#!/bin/bash
#SBATCH --job-name=test_python_job
#SBATCH --time=00:15:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4

hostname
source /home/kdirker/Thesis/master-thesis/transpile/PWF/env/bin/activate
python -V
python3 -V
python3.11 -V

OUT=out.png
NOISE=no_noise_image.fits
NOISEOUT=no_noise_out.png
TMPDIR=tmp_dir
OUTDIR=out_dir

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

# Execute the tools simulating the workflow in the workflow directory
# python download_images.py download_images.yaml    < Doesn't work because url is broken
python3 imageplotter.py -y imageplotter.yaml --preserve_tmp --tmpdir $TMPDIR --outdir $OUTDIR --dask
python3 noiseremover.py -y noiseremover.yaml --preserve_tmp --tmpdir $TMPDIR --outdir $OUTDIR --dask
python3 imageplotter.py -y imageplotter_after.yaml --preserve_tmp --tmpdir $TMPDIR --outdir $OUTDIR --dask
