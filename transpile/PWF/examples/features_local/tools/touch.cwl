cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo 

inputs:
  - id: filename
    type: string
    inputBinding:
        position: 0

stdout: $(inputs.filename)

outputs:
  file_name:
    type: stdout
  file_content:
    type:
      type: array
      items: string
    outputBinding:
      loadContents: true
      glob: $(inputs.filename)
      outputEval: $(self[0].contents.trim().split())

requirements:
  - class: InlineJavascriptRequirement
