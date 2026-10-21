class: CommandLineTool
cwlVersion: v1.2
id: imageplotter
baseCommand:
  - /var/scratch/kdirker/master-thesis/experiments/run_test/process_images/env/bin/python3.11
  - imageplotter.py
inputs:
  - id: input_fits
    type: File[]
    inputBinding:
      position: 0
  - id: output_image
    type: string
    inputBinding:
      position: 1
outputs:
  - id: output
    type: File
    outputBinding:
      glob: $(inputs.output_image)
label: imageplotter
requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - class: File
        path: imageplotter.py
