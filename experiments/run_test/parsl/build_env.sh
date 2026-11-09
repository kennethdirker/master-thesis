if [ -d "env" ]; then
    echo "'env' already exists! Remove before trying again."
    exit
fi


# /var/scratch/kdirker/python3.11/bin/python3.11 -m venv env
python3.12 -m venv --copies env

source ./env/bin/activate
python -m pip install pip --upgrade
python -m pip install -r requirements.txt
python -m pip install -r /var/scratch/kdirker/master-thesis/transpile/Transpiler/example_requirements.txt
