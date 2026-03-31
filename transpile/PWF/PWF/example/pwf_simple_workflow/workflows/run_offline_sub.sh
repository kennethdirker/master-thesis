# Runs the offline version of the workflow, skipping the download_images step.
# Expect the fits to already be present in the working directory.

OUTDIR="./output_offline"
TMPDIR="./tmp_offline"

if [ ! -f "./image_mf_01.fits" ] || [ ! -f "./image_mf_02.fits" ] || [ ! -f "./image_mf_04.fits" ]; then
    echo "Missing input fit(s)"
    exit
fi

if test -d $OUTDIR; then rm -rf $OUTDIR; fi
if test -d $TMPDIR; then rm -rf $TMPDIR; fi
if test -f no_noise_image_mf_01.fits; then rm no_noise_image_mf_01.fits; fi
if test -f no_noise_image_mf_02.fits; then rm no_noise_image_mf_02.fits; fi
if test -f no_noise_image_mf_04.fits; then rm no_noise_image_mf_04.fits; fi

if test -f after_noise_remover.png; then rm after_noise_remover.png; fi
if test -f before_noise_remover.png; then rm before_noise_remover.png; fi

python process_images_offline_subworkflows.py -y process_images_offline_subworkflows.yaml --outdir $OUTDIR --tmpdir $TMPDIR --preserve_tmp
# python process_images_offline_subworkflows.py -y process_images_offline_subworkflows.yaml --outdir $OUTDIR --tmpdir $TMPDIR --preserve_tmp --dask