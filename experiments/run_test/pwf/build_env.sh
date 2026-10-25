if [ -d "env" ]; then
    echo "'env' already exists! Remove before trying again."
    exit
fi


/var/scratch/kdirker/python3.11/bin/python3.11 -m venv env
source ./env/bin/activate
pip3 install -r /var/scratch/kdirker/master-thesis/transpile/Transpiler/requirements.txt
pip3 install -e /var/scratch/kdirker/master-thesis/transpile/Transpiler
pip3 install -r /var/scratch/kdirker/master-thesis/transpile/Transpiler/example_requirements.txt
