class: Workflow
cwlVersion: v1.2
id: scatter_no_op_wf
inputs: 
    - id: i
      type: int
      default: 10
outputs:
    - id: output_int
      type: int
      outputSource:
        - recursive_step/output_int
        - tool_step/output_int
      pickValue: first_non_null
steps:
  - id: recursive_step
    in:
        - id: input_int
          source: i
          valueFrom: $(self - 1)
    out: [output_int]
    run: noop.cwl

  - id: tool_step
    in:
        - id: input_int
          source: i
    out: [output_int]
    run: noop.cwl
    when: $(inputs.i === 0)

requirements:
  - class: ScatterFeatureRequirement
  - class: SubworkflowFeatureRequirement
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement
