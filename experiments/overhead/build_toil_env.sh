ENV_NAME=toil_env

if [ -d $ENV_NAME ]; then
    echo "'$ENV_NAME' already exists! Remove before trying again."
    exit
fi

python3.12 -m venv --copies $ENV_NAME
source ./$ENV_NAME/bin/activate
python3.12 -m pip install pip --upgrade
python3.12 -m pip install toil[cwl]
