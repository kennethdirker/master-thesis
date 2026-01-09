class: CommandLineTool
cwlVersion: v1.2
id: noiseremover
baseCommand:
  - python
  - /home/kennethdirker/Leiden/2024-2025/Thesis/transpile/PWF/PWF/example/cwl_simple_workflow/scripts/noiseremover.py
inputs:
  - id: input
    type: File
    inputBinding:
      position: 0
  - id: output_file_name
    type: string
    inputBinding:
      position: 1
outputs:
  - id: output
    type: File
    outputBinding:
      glob: $(inputs.output_file_name)
label: noiseremover
requirements:
  - class: InlineJavascriptRequirement
