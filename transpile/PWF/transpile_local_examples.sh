# cwl_local_workflow
python src/PWF/transpiler.py -i ./examples/cwl_local_workflow/steps/*.cwl -d ./examples/pwf_local_workflow/steps
python src/PWF/transpiler.py -i ./examples/cwl_local_workflow/workflows/*.cwl -d ./examples/pwf_local_workflow/workflows

# Features
python src/PWF/transpiler.py -i ./examples/features_local/tools/*.cwl -s
python src/PWF/transpiler.py -i ./examples/features_local/workflows/*.cwl -s