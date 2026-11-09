TRANSPILER=/var/scratch/kdirker/master-thesis/transpile/Transpiler/src/CWL2DASK/transpiler.py

source pwf_env/bin/activate

# if [ ! -f serial_wf.py ]; then python $TRANSPILER --slurm -ci serial_wf.cwl -o serial_wf.py; fi
if [ ! -f scatter_wf.py ]; then python $TRANSPILER -ci scatter_wf.cwl -o scatter_wf.py; fi
if [ ! -f recursive_wf.py ]; then python $TRANSPILER -ci recursive_wf.cwl -o recursive_wf.py; fi
# if [ ! -f scatter_wf.py ]; then python $TRANSPILER --slurm -ci scatter_wf.cwl -o scatter_wf.py; fi
# if [ ! -f recursive_wf.py ]; then python $TRANSPILER --slurm -ci recursive_wf.cwl -o recursive_wf.py; fi

python \
    scatter_wf.py \
    input_object.yaml