id: listdirectory
label: list_directory
class: ExpressionTool
cwlVersion: v1.2

inputs:
  - id: input
    type: Directory
    loadListing: shallow_listing
outputs:
  - id: output
    type: File[]

expression: |
  ${
    return {'output': inputs.input.listing}
  }

requirements:
  - class: InlineJavascriptRequirement
