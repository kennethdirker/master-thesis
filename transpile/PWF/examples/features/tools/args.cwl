cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo

inputs:
  world:
    type: string
  beautiful:
    type: string

arguments:
  - valueFrom: Hello
  - valueFrom: $(inputs.world)
    position: 2
  - valueFrom: $(inputs.beautiful)
    position: 1

stdout: output.txt

outputs:
  output:
    type: stdout






