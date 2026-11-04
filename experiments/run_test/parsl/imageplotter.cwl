class: CommandLineTool
cwlVersion: v1.2
id: imageplotter
baseCommand:
  - python
  - /var/scratch/kdirker/master-thesis/experiments/run_test/parsl/imageplotter.py
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
      glob: before_noise_remover.png
label: imageplotter
requirements:
  - class: InlineJavascriptRequirement
