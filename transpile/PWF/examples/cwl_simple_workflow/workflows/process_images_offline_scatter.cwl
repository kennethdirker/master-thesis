class: Workflow
cwlVersion: v1.2
id: process_images
label: process_images
inputs:
  - id: fit_list
    type: File[]
outputs:
  - id: before_noise_remover
    outputSource:
      - imageplotter/output
    type: File
  - id: after_noise_remover_plot
    outputSource:
      - after_plot_inspect/output
    type: File
steps:
  - id: imageplotter
    in:
      - id: input_fits
        source:
          - fit_list
      - id: output_image
        default: before_noise_remover.png
    out:
      - id: output
    run: ../steps/imageplotter.cwl
    label: imageplotter
  - id: noiseremover
    in:
      - id: input
        source: fit_list
      - id: output_file_name
        valueFrom: $('no_noise_' + inputs.input.basename)
    out:
      - id: output
    run: ../steps/noiseremover.cwl
    label: noiseremover
    scatter:
      - input
  - id: after_plot_inspect
    in:
      - id: input_fits
        source:
          - noiseremover/output
      - id: output_image
        default: after_noise_remover.png
    out:
      - id: output
    run: ../steps/imageplotter.cwl
    label: imageplotter
requirements:
  - class: ScatterFeatureRequirement
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement
