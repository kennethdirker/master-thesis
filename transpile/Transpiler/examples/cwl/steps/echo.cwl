#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool
id: echo
label: echo_to_stdout
doc: Tool that writes a greeting to the user to stdout.
baseCommand: echo
arguments: 
    - valueFrom: "Greetings,"
      position: 0
inputs:
  username:
    type: string
    inputBinding:
      position: 1
outputs:
  - id: outfile
    type: stdout
  - id: outmessage
    type: string
    outputBinding:
     glob: greeting.txt
     outputEval: $(self.contents)
stdout: greeting.txt