# cwl_simple_workflow
python src/transpiler.py -i ./example/cwl_simple_workflow/steps/*.cwl -d ./example/pwf_simple_workflow/steps
python src/transpiler.py -i ./example/cwl_simple_workflow/workflows/*.cwl -d ./example/pwf_simple_workflow/workflows

# Features
# python src/transpiler.py -i ./example/features/tools/*.cwl ./example/features/workflows/*.cwl -s
python src/transpiler.py -i ./example/features/tools/*.cwl -s
# python src/transpiler.py -i ./example/features/workflows/*.cwl -s