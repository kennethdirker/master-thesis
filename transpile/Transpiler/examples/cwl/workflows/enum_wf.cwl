class: Workflow
cwlVersion: v1.2
id: enum_wf
inputs:
    - id: foo
      type: 
        - type: enum
          symbols:
            - "a"
            - "b"
            - "c"
outputs:
  - id: wf_output
    outputSource:
      - enum/out
    type: File
steps:
  - id: enum
    in:
      - id: bound
        source:
          - foo
    out:
      - id: out
    run: ../steps/enum.cwl