# Transpile each CWL workflow and tool to a Python DASK script
# 
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/download_images.cwl -o examples/generated/download_images.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/env.cwl -o examples/generated/env.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/hostname.cwl -o examples/generated/hostname.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/imageplotter.cwl -o examples/generated/imageplotter.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/InitialWorkDirRequirement.cwl -o examples/generated/InitialWorkDirRequirement.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/multitype.cwl -o examples/generated/multitype.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/noiseremover.cwl -o examples/generated/noiseremover.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/print.cwl -o examples/generated/print.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/stdout.cwl -o examples/generated/stdout.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/steps/touch.cwl -o examples/generated/touch.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/workflows/process_images.cwl -o examples/generated/process_images.py
python src/CWL2DASK/transpiler.py -ci examples/cwl/workflows/process_images_sub.cwl -o examples/generated/process_images_sub.py