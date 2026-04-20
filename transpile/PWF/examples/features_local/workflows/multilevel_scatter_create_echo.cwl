class: Workflow
cwlVersion: v1.2
id: multilevel_scatter_2_example
label: multilevel scatter example
inputs:
  - id: str
    type: string
  - id: num
    type: int

steps:
    - id: touch
      in:
      - id: str
        source: str
      - id: num
        source: num
      - id: filename
        valueFrom: $(inputs.str + inputs.num)
      out: [file_name, file_content]
      run: ../tools/touch.cwl

    - id: print
      in:
      - id: str
        source: touch/file_content
      out: [echo]
      scatter:
        - str
      run: ../tools/print.cwl

outputs:
    - id: output_files
      outputSource: 
        - touch/file_name
      type: File
    - id: content_logs
      outputSource: 
        - print/echo
      type: string[]

requirements:
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement
  - class: ScatterFeatureRequirement
