cwlVersion: v1.2
class: CommandLineTool
id: env
baseCommand: env
requirements:
  EnvVarRequirement:
    envDef:
      HELLO: $(inputs.message)
inputs:
  message: string
outputs:
  example_out:
    type: stdout
stdout: output.txt