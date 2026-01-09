class: CommandLineTool
cwlVersion: v1.2
id: imageplotter
baseCommand:
  - python
  - /home/kennethdirker/Leiden/2024-2025/Thesis/transpile/PWF/PWF/example/cwl_simple_workflow/scripts/imageplotter.py
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
