from PWF.src.commandlinetool import BaseCommandLineTool

class tool_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {}

	def set_inputs(self):
		self.inputs = {
			"max_dp3_threads": {
				"type": ["int", "null"],
				"bound": True,
				"position": 0,
				"prefix": "numthreads=",
			},
			"msin": {
				"type": "directory",
				"bound": True,
				"position": 0,
				"prefix": "msin=",
				"doc": "Input Measurement Set",
			},
			"msin_datacolumn": {
				"type": "string",
				"default": "DATA",
				"bound": True,
				"position": 0,
				"prefix": "msin.datacolumn=",
				"doc": "Input data Column",
			},
			"msout_datacolumn": {
				"type": "string",
				"bound": True,
				"position": 0,
				"prefix": "msout.datacolumn=",
				"doc": "Output data column",
			},
			"type": {
				"type": ["string", "null"],
				"default": "applybeam",
				"bound": True,
				"position": 0,
				"prefix": "applybeam.type=",
				"doc": \
					"Type of correction to perform.  When using H5Parm, this" \
					"is for now the name of the soltab; the type will be" \
					"deduced from the metadata in that soltab, except for" \
					"full Jones, in which case correction should be" \
					"'fulljones'."
			},
			"storagemanager": {
				"type": "string",
				"default": "",
				"bound": True,
				"prefix": "msout.storagemanager=",
			},
			"databitrate": {
				"type": ["int", "null"],
				"bound": True,
				"prefix": "msout.storagemanager.databitrate=",
			},
			"updateweights": {
				"type": ["boolean", "null"],
				"bound": True,
				"position": 0,
				"prefix": "applybeam.updateweights=True",
			},
			"usechannelfreq": {
				"type": ["boolean", "null"],
				"default": "True",
				"bound": True,
				"position": 0,
				"prefix": "applybeam.usechannelfreq=False",
				"valueFrom": "$(!self)",
			},
			"invert": {
				"type": ["boolean", "null"],
				"default": "True",
				"bound": True,
				"position": 0,
				"prefix": "applybeam.invert=False",
				"valueFrom": "$(!self)",
			},
			"beammode": {
				"type": ["string", "null"],
				"bound": True,
				"position": 0,
				"prefix": "applybeam.beammode=",
			},
		}

	def set_outputs(self):
		self.outputs = {
			"msout": {
				"type": "directory",
				"glob": "$(inputs.msin.basename)",
			},
			"logfile": {
				"type": "file[]",
				"glob": "$('applycal_' + inputs.type)",
			},
		}

	def set_base_command(self):
		self.base_command = [
			"DP3",
		]

	def set_requirements(self):
		self.requirements = {
			"InplaceUpdateRequirement": True,
			"ResourceRequirement": {
				"coresMin": "$(inputs.max_dp3_threads)",
			},
			"EnvVarRequirement": {
				"DP3_CACHE_DIR": "$(inputs.storagemanager)",
				"DP3_DATA_DIR": "/data",
			},
		}

	def set_io(self):
		self.io = {
			"stdout": "$('applycal_' + inputs.type)",
			"stderr": "$('applycal_' + inputs.type)",
		}

if __name__ == "__main__":
	tool_PWF()