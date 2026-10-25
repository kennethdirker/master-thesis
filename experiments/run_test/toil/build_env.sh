if [ -d "env" ]; then
    echo "'env' already exists! Remove before trying again."
    exit
fi


/var/scratch/kdirker/python3.11/bin/python3.11 -m venv env
source ./env/bin/activate
pip3 install -r requirements.txt
