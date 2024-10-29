class: CommandLineTool
cwlVersion: v1.2
id: make_parset
label: define_parset
inputs:
  - id: raw_data
    type: boolean?
    default: false
  - id: demix
    type: boolean?
    default: false
  - id: filter_baselines
    type: string?
    default: '[CR]S*&'
  - id: memoryperc
    type: int?
    default: 20
  - id: baselines_to_flag
    type: string[]?
    default: []
  - id: elevation_to_flag
    type: string?
    default: '0deg..15deg'
  - id: min_amplitude_to_flag
    type: float?
    default: 1e-30
  - id: timeresolution
    type: int?
    default: 4
  - id: freqresolution
    type: string?
    default: '48.82kHz'
  - id: process_baselines_cal
    type: string?
    default: '*&'
  - id: demix_timeres
    type: float?
    default: 10
  - id: demix_freqres
    type: string?
    default: '48.82kHz'
  - id: target_source
    type: string?
    default: ''
  - id: subtract_sources
    type: string[]?
    default:
      - CasA
      - CygA
  - id: ntimechunk
    type: int?
    default: 10
  - id: lbfgs_historysize
    type: int?
    default: 10
  - id: lbfgs_robustdof
    type: float?
    default: 200

outputs:
  - id: output
    type: stdout

baseCommand:
  - cat
  - input.parset

stdout: DP3.parset

requirements:
  - class: InlineJavascriptRequirement
  - class: ShellCommandRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: input.parset
        entry: |+
          steps                               =  [filter,count,$(inputs.raw_data?'flagedge,aoflag,':'')flagbaseline,flagelev,flagamp,$(inputs.demix?'demix,':'')avg]
          #
          msout.storagemanager                =   "Dysco"
          msout.storagemanager.databitrate    =   0
          #
          filter.type                         =   filter
          filter.baseline                     =   $(inputs.filter_baselines)
          filter.remove                       =   true
          #
          flagedge.type                       =   preflagger
          flagedge.chan                       =   [0..nchan/32-1,31*nchan/32..nchan-1]
          #
          aoflag.type                         =   aoflagger
          aoflag.memoryperc                   =   $(inputs.memoryperc)
          aoflag.keepstatistics               =   false
          #
          flagbaseline.type                   =   preflagger
          flagbaseline.baseline               =   $(inputs.baselines_to_flag)
          #
          flagelev.type                       =   preflagger
          flagelev.elevation                  =   $(inputs.elevation_to_flag)
          #
          flagamp.type                        =   preflagger
          flagamp.amplmin                     =   $(inputs.min_amplitude_to_flag)
          #
          avg.type                            =   average
          avg.timeresolution                  =   $(inputs.timeresolution)
          avg.freqresolution                  =   $(inputs.freqresolution)
          #
          demix.type                          =   demixer
          demix.baseline                      =   $(inputs.process_baselines_cal)
          demix.demixfreqresolution           =   $(inputs.demix_freqres)
          demix.demixtimeresolution           =   $(inputs.demix_timeres)
          demix.ignoretarget                  =   False
          demix.targetsource                  =   $(inputs.target_source)
          demix.subtractsources               =   $(inputs.subtract_sources)
          demix.ntimechunk                    =   $(inputs.ntimechunk)
          demix.freqstep                      =   1
          demix.timestep                      =   1
          demix.instrumentmodel               =   instrument
          demix.uselbfgssolver                =   True
          demix.lbfgs.historysize             =   $(inputs.lbfgs_historysize)
          demix.lbfgs.robustdof               =   $(inputs.lbfgs_robustdof)
          demix.maxiter                       =   20

