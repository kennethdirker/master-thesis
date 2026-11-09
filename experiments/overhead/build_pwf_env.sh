ENV_NAME=pwf_env

if [ -d $ENV_NAME ]; then
    echo "'$ENV_NAME' already exists! Remove before trying again."
    exit
fi

python3.12 -m venv --copies $ENV_NAME

source ./$ENV_NAME/bin/activate
python -m pip install pip --upgrade
python -m pip install cwl_utils==0.35 \
                      dask \
                      distributed \
                      dask-jobqueue \
                      Js2Py==0.74 \
                      PyYAML==6.0.3
python -m pip install -e /var/scratch/kdirker/master-thesis/transpile/Transpiler

# Add js2py fix for Python 3.12 support
cp /var/scratch/kdirker/master-thesis/experiments/injector.py \
   /var/scratch/kdirker/master-thesis/experiments/overhead/$ENV_NAME/lib/python3.12/site-packages/js2py/utils/injector.py
cp /var/scratch/kdirker/master-thesis/experiments/translating_nodes.py \
   /var/scratch/kdirker/master-thesis/experiments/overhead/$ENV_NAME/lib/python3.12/site-packages/js2py/translators/translating_nodes.py
