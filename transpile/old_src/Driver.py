import argparse, pathlib



def main(cwl_file: str, inputs_file: str):
    """
    """
    runner = Runner(cwl_file, inputs_file)
    runner.visualize_graph()
    runner.execute_graph()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog=program_name,
                    description="Execute a cwl workflow with DASK.",
                    epilog="")
    parser.add_argument("-f", "--file", required=True)
    parser.add_argument("-i", "--input", required=False)
    args = parser.parse_args()

    inputs = args.input if args.input else None

    if args.file:
        main(args.file, inputs)
    else:
        raise Exception("No workflow file given. Use '-f' or '--file' argument!")