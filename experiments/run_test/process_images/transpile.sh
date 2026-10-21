source env/bin/activate
# python3 /var/scratch/kdirker/master-thesis/transpile/Transpiler/src/CWL2DASK/transpiler.py -c -i process_images_sub.cwl -o process_images_sub.py
python3 /var/scratch/kdirker/master-thesis/transpile/Transpiler/src/CWL2DASK/transpiler.py -c --slurm -i process_images_sub.cwl -o process_images_sub.py
# python3 /var/scratch/kdirker/master-thesis/transpile/Transpiler/src/CWL2DASK/transpiler.py -c -i process_images.cwl -o process_images.py
python3 /var/scratch/kdirker/master-thesis/transpile/Transpiler/src/CWL2DASK/transpiler.py -c --slurm -i process_images.cwl -o process_images.py
