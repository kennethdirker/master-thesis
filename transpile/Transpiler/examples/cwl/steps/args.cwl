cwlVersion: v1.2
class: CommandLineTool
id: args
baseCommand: echo

inputs:
  world:
    type: string
  beautiful:
    type: string

arguments:
  - valueFrom: Hello
  - |
    This is some
    Lorem Ipsum
    type $(inputs.food + "digestion") stuff
    Peace out!
  - valueFrom: $(inputs.world)
    position: 2
  - valueFrom: $(inputs.beautiful)
    prefix: "#"
    position: 1
  - valueFrom: Bazinga
    position: 8

stdout: output.txt

outputs:
  output:
    type: stdout






