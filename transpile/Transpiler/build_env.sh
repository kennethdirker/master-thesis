python3.11 -m venv env

source ./env/bin/activate
python -m pip install pip --upgrade

python -m pip install -r requirements.txt
python -m pip install -r example_requirements.txt
python -m pip install -e .
