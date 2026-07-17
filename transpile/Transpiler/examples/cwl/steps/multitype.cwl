cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo
id: mutlitype_example
inputs:
  message:
    type:
        - int?
        - string
    inputBinding:
        position: 0
outputs: []