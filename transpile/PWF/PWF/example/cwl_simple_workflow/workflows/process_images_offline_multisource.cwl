class: Workflow
cwlVersion: v1.2
id: process_images
label: process_images
inputs:
  - id: fit_list
    type: File[]
  - id: fit_1
    type: File
  - id: fit_2
    type: File
  - id: fit_3
    type: File
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
  - id: noiseremover1
    in:
      - id: input
        source: fit_1
      - id: output_file_name
        valueFrom: $('no_noise_' + inputs.input.basename)
    out:
      - id: output
    run: ../steps/noiseremover.cwl
    label: noiseremover
  - id: noiseremover2
    in:
      - id: input
        source: fit_2
      - id: output_file_name
        valueFrom: $('no_noise_' + inputs.input.basename)
    out:
      - id: output
    run: ../steps/noiseremover.cwl
    label: noiseremover
  - id: noiseremover3
    in:
      - id: input
        source: fit_3
      - id: output_file_name
        valueFrom: $('no_noise_' + inputs.input.basename)
    out:
      - id: output
    run: ../steps/noiseremover.cwl
    label: noiseremover
  - id: after_plot_inspect
    in:
      - id: input_fits
        source:
          - noiseremover1/output
          - noiseremover2/output
          - noiseremover3/output
        linkMerge: merge_nested
      - id: output_image
        default: after_noise_remover.png
    out:
      - id: output
    run: ../steps/imageplotter.cwl
    label: imageplotter
requirements:
  - class: MultipleInputFeatureRequirement
  - class: StepInputExpressionRequirement
  - class: InlineJavascriptRequirement
