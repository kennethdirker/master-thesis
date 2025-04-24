class: Workflow
cwlVersion: v1.2
id: fillhere
label: fillhere
inputs: []
outputs: []
steps: 
  - id: step_one
    in: 
      - id: input_one
        source:
          - sourcename
    out:
      - id: output_id 
    run: ./imageplotter.cwl
    label: imageplotter
requirements:
  - class: ScatterFeatureRequirement
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement
