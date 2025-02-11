import argparse, pathlib
import Client


PROGRAM_NAME = "Placeholder Name"



def main(cwl_file: str, input_object: str):
    """
    """
    client = Client()
    client.load_cwl_object(cwl_file)
    client.load_input_object(input_object)
    client.visualize_graph()
    client.execute_graph()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog=PROGRAM_NAME,
                    description="Execute CWL objects with DASK.",
                    epilog="")
    parser.add_argument("-c", "--cwl", required=True)
    parser.add_argument("-i", "--input", required=True)
    args = parser.parse_args()


    if args.cwl and args.input:
        main(args.cwl, args.input)
    else:
        raise Exception("No workflow file given. Use '-f' or '--file' argument!")