
ENV_NAME=env

if [ -d $ENV_NAME ]; then
    echo "'$ENV_NAME' already exists! Remove before trying again."
    exit
fi


# /var/scratch/kdirker/python3.11/bin/python3.11 -m venv env
python3.12 -m venv --copies $ENV_NAME

source $ENV_NAME/bin/activate
python -m pip install pip --upgrade
python -m pip install -r /var/scratch/kdirker/master-thesis/transpile/Transpiler/requirements.txt
python -m pip install -e /var/scratch/kdirker/master-thesis/transpile/Transpiler
python -m pip install -r /var/scratch/kdirker/master-thesis/transpile/Transpiler/example_requirements.txt


# Add js2py fix for Python 3.12 support
cp /var/scratch/kdirker/master-thesis/experiments/injector.py \
   $PWD/$ENV_NAME/lib/python3.12/site-packages/js2py/utils/injector.py
cp /var/scratch/kdirker/master-thesis/experiments/translating_nodes.py \
   $PWD/$ENV_NAME/lib/python3.12/site-packages/js2py/translators/translating_nodes.py

