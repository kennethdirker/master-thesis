from dask.distributed import Client
from subprocess import run, CompletedProcess

class Command:
    def __init__(self, cmd: list[str]):
        self.cmd = cmd

    def __call__(self):
        return run(self.cmd, capture_output=True)
    
def test_class(client):
    cmd = ["echo", "hello", "world"]
    cmd = Command(cmd)
    future = client.submit(cmd)
    print(future.result().stdout.decode())



class Runner:
    def __init__(self, client):
        self.client = client
        self.runtime_vars = {}

    def execute(self, cmd, inputs) -> CompletedProcess:
        def wrapper(cmd, inputs, runtime_vars, p):
            cmd += [runtime_vars[i] for i in inputs]
            p(cmd)
            return run(cmd, capture_output=True)
        def p(cmd):
            print("Running: " + " ".join(cmd))
        
        future = self.client.submit(wrapper, cmd, inputs, self.runtime_vars, p)
        return future.result()

def test_inner_function(client):
    cmd = ["echo"]
    runner = Runner(client)
    runner.runtime_vars = {
        "a": "A",
        "b": "B",
        "c": "C",
        "d": "D",
    }
    inputs = ["b", "d"]
    result = runner.execute(cmd, inputs)
    print(result.stdout.decode("utf-8"))

if __name__ == "__main__":
    client = Client()
    test_inner_function(client)
    test_class(client)
    