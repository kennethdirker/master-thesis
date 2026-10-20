cwlVersion: v1.2
class: CommandLineTool
id: capitalize
baseCommand: ":"    # <- no op
inputs:
  str:
    type: string
outputs:
  output:
    type: string
    outputBinding:
      outputEval: $(inputs.str.toUpperCase())
stdout: out.txt
  
requirements: 
  - class: ShellCommandRequirement
  - class: InlineJavascriptRequirement