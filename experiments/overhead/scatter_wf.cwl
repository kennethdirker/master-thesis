class: Workflow
cwlVersion: v1.2
id: scatter_no_op_wf
inputs: 
    - id: range
      type: int[]
      default: [0,1,2,3,4,5,6,7,8,9]
outputs:
    - id: output_int
      type: int[]
      outputSource: scatter_step/output_int
steps:
  - id: scatter_step
    in:
        - id: input_int
          source: range
    out: [output_int]
    run: noop.cwl
    scatter:
      - input_int
requirements:
  - class: ScatterFeatureRequirement
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement
