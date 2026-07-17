class: CommandLineTool
cwlVersion: v1.2
$namespaces:
  sbg: 'https://www.sevenbridges.com/'
id: download_images
baseCommand:
  - wget
inputs:
  - id: url_list
    type: File
    inputBinding:
      position: 0
      prefix: '-i'
outputs:
  - id: output
    type: 'File[]'
    outputBinding:
      glob: '*.fits'
label: download_images
