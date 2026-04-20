OUT=out.png
NOISE=no_noise_image.fits
NOISEOUT=no_noise_out.png
TMP=tmp_dir

if test -f $OUT; then
    rm $OUT
fi
if test -f $NOISE; then
    rm $NOISE
fi
if test -f $NOISEOUT; then
    rm $NOISEOUT
fi
if test -d $TMP; then
    rm -rf $TMP
fi

# Execute the tools simulating the workflow in the workflow directory
# python download_images.py download_images.yaml    < Doesn't work because url is broken
python imageplotter.py -y imageplotter.yaml --preserve_tmp --tmpdir $TMP
python noiseremover.py -y noiseremover.yaml --preserve_tmp --tmpdir $TMP
python imageplotter.py -y imageplotter_after.yaml --preserve_tmp --tmpdir $TMP
