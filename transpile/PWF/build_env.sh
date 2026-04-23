PYTHON=/var/scratch/kdirker/python3.11/bin/python3
$PYTHON -V
which $PYTHON
$PYTHON -m venv env
source env/bin/activate
python3 -m pip install -r test_requirements.txt