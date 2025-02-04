class: CommandLineTool
cwlVersion: v1.2
id: getFWHM
baseCommand:
  - python3
arguments:
    - position: 0
      valueFrom: getFWHM.py
inputs:
  - id: beamfreq
    type: float?
    default: 50e6
  - id: msin
    type: Directory[]
    inputBinding:
      position: 1
outputs:
  - id: fwhm
    type: float
    outputBinding:
      loadContents: true
      glob: 'out.json'
      outputEval: '$(JSON.parse(self[0].contents).FWHM)'
  - id: msout
    doc: Output MS
    type: Directory[]
    outputBinding:
      outputEval: $(inputs.msin)
label: getFWHM

requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
     - entryname: getFWHM.py
       entry: |
        import sys
        import json
        import numpy
        from casacore import tables

        inputs = json.loads(r"""$(inputs)""")
        beamfreq = inputs['beamfreq']
        ms_list = sys.argv[1:]

        # Following numbers are based at 60 MHz (old.astron.nl/radio-observatory/astronomers/lofar-imaging-capabilities-sensitivity/lofar-imaging-capabilities/lofa)

        with tables.table(ms_list[0]+'/OBSERVATION', ack = False) as t:
            antenna_set = t.getcell("LOFAR_ANTENNA_SET",0)
        scale = 60e6/beamfreq

        if 'HBA' in antenna_set:
             fwhm = 10.0
        elif 'OUTER' in antenna_set:
             fwhm = 3.88 * scale
        elif 'SPARSE' in antenna_set:
             fwhm = 4.85 * scale
        elif 'INNER' in antenna_set:
             fwhm = 9.77 * scale

        cwl_output = {"FWHM": fwhm}

        with open('./out.json', 'w') as fp:
             json.dump(cwl_output, fp)
                
hints:
  - class: DockerRequirement
    dockerPull: astronrd/linc

stdout: getFWHM.log
stderr: getFWHM_err.log
