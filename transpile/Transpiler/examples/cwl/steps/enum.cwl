cwlVersion: v1.2
class: CommandLineTool
id: enum
baseCommand: echo

inputs:
    - id: bound
      type: 
        - type: enum
          symbols:
            - "a"
            - "b"
            - "c"
      inputBinding:
        position: 1

outputs:
    - id: out
      type:
        - type: enum
          symbols:
            - "a"
            - "b"
            - "c"
      outputBinding:
        glob: enum.stdout
        loadContents: true
        outputEval: $(self[0].contents)

stdout: enum.stdout

