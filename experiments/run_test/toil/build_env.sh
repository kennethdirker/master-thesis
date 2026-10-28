if [ -d "env" ]; then
    echo "'env' already exists! Remove before trying again."
    exit
fi


# /var/scratch/kdirker/python3.11/bin/python3.11 -m venv env
python3.12 -m venv --copies env
source ./env/bin/activate
python3.12 -m pip install pip --upgrade
python3.12 -m pip install -r requirements.txt
python3.12 -m pip install -r /var/scratch/kdirker/master-thesis/transpile/Transpiler/example_requirements.txt
