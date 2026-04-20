# cwl_local_workflow
python src/transpiler.py -i ./examples/cwl_local_workflow/steps/*.cwl -d ./examples/pwf_local_worklfow/steps
python src/transpiler.py -i ./examples/cwl_local_workflow/workflows/*.cwl -d ./examples/pwf_local_worklfow/workflows

# Features
python src/transpiler.py -i ./examples/features/tools/*.cwl -s
python src/transpiler.py -i ./examples/features/workflows/*.cwl -s