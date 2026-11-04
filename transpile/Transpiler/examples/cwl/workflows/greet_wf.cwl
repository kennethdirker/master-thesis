class: Workflow
cwlVersion: v1.2
id: greet_wf
inputs:
  - id: username
    type: string
outputs:
  - id: greeting
    outputSource:
      - capitalize_string/output
    type: File
steps:
  - id: greet_smith
    in:
      - id: username
        source:
          - username
      - id: surname
        default: Smith
    out:
      - id: outfile
      - id: outmessage
    run: ../steps/greet.cwl
  - id: capitalize_string
    in:
      - id: outmessage
        source: greet_smith/outmessage
    out:
      - id: output
    run: ../steps/capitalize.cwl
    label: capitalize
