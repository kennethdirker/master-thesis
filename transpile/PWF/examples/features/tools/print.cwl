cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo

inputs:
  - id: str
    type: string
    inputBinding:
        position: 0


stdout: $(inputs.str)

outputs:
    echo:
        type: string
        outputBinding:
            loadContents: true
            glob: $(inputs.str)
            outputEval: $(self[0].contents.trim())

requirements: 
    InlineJavascriptRequirement: {}
