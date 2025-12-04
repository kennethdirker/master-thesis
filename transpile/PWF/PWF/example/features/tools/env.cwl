cwlVersion: v1.2
class: CommandLineTool
baseCommand: env
requirements:
  EnvVarRequirement:
    envDef:
      HELLO: $(inputs.message)
inputs:
  message: string
  message2:
    type: string
    default: "WORLD"
outputs: []