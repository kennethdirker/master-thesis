cwlVersion: v1.2
class: CommandLineTool
baseCommand: 
  echo
  Hello from within
inputs:
  message:
    type: string
    # inputBinding: {}
outputs:  []

#   out:
#     type: string
#     outputBinding:
#       glob: output.txt
#       loadContents: true
#       outputEval: $(self[0].contents)