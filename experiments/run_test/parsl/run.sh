source env/bin/activate
rm -rf runinfo/*
if test -f before_noise_remover.png ; then rm  before_noise_remover.png ; fi
python process_images.py