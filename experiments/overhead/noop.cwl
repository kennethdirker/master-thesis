cwlVersion: v1.2
class: CommandLineTool
id: no_op
baseCommand: ":"    # <- no op
inputs:
  - id: input_int
    type: int
outputs:
  - id: output_int
    type: int
    outputBinding:
      outputEval: $(inputs.input_int) 
requirements:
  - class: ShellCommandRequirement