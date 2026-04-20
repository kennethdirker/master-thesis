# cwl_local_workflow
python3 src/PWF/transpiler.py -i ./examples/cwl_local_workflow/steps/*.cwl -d ./examples/pwf_cluster_workflow/steps
python3 src/PWF/transpiler.py -i ./examples/cwl_local_workflow/workflows/*.cwl -d ./examples/pwf_cluster_workflow/workflows

# Features
python3 src/PWF/transpiler.py -i ./examples/features_cluster/tools/*.cwl -s
python3 src/PWF/transpiler.py -i ./examples/features_cluster/workflows/*.cwl -s