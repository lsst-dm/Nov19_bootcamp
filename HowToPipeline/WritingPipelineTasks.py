import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage

import numpy as np

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires


class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                              dimensions=("visit", "detector", "abstract_filter", "skymap")):
    exposure = pipeBase.connectionTypes.Input(doc="Input exposure to make measurements on",
                                              dimensions=("instrument", "visit", "detector"),
                                              storageClass="ExposureF",
                                              name="calexp")
    inputCatalog = pipeBase.connectionTypes.Input(doc="Input catalog with existing measurements",
                                                  dimensions=("instrument", "visit", "detector",),
                                                  storageClass="SourceCatalog",
                                                  name="src") 
    outputCatalog = pipeBase.connectionTypes.Output(doc="Aperture measurements",
                                                    dimensions=("visit", "detector", "abstract_filter",
                                                                "skymap"),
                                                    storageClass="SourceCatalog",
                                                    name="customAperture")


class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                         pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=float, default=4)


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    _DefaultName = "apertureTask"

    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.apRad = self.config.apRad

        self.outputSchema = afwTable.SourceTable.makeMinimalSchema()
        self.apKey = self.outputSchema.addField("apFlux", type=np.float64, doc="Ap flux measured")

        self.outputCatalog = afwTable.SourceCatalog(self.outputSchema)

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog, msg: str) -> pipeBase.Struct:
        self.log.info(f"Running aperture phot {msg}")
        # Get the dimensions of the exposure
        dimensions = exposure.getDimensions()

        # Get indexes for each pixel
        indy, indx = np.indices((dimensions.getY(), dimensions.getX()))

        # Loop over each record in the catalog
        for i, source in enumerate(inputCatalog):
            if i%100 == 0:
                self.log.info(f"Processing record {i}")
            # Create an aperture and measure the flux
            center = source.getCentroid()
            mask = ((indy - center.getY())**2 + (indx - center.getX())**2)**0.5 < self.apRad
            flux = np.sum(exposure.image.array*mask)

            # Add a record to the output catalog
            tmpRecord = self.outputCatalog.addNew()
            tmpRecord.set(self.apKey, flux)

        return pipeBase.Struct(outputCatalog=self.outputCatalog)

    def runQuantum(self, butlerQC: pipeBase.ButlerQuantumContext,
                   inputRefs: pipeBase.InputQuantizedConnection,
                   outputRefs: pipeBase.OutputQuantizedConnection):
        inputs = butlerQC.get(inputRefs)
        # Add an extra argument that is to be passed to run, this is not part of
        # the base class runQuantum
        inputs['msg'] = str(butlerQC.quantum.dataId)
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)
# PYTHONPATH=$PYTHONPATH:$(pwd) pipetask run -j 5 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i shared/ci_hsc_output -o aptest2 --register-dataset-types -t WritingPipelineTasks.ApertureTask -d "detector in (22, 16, 23) AND visit in (90334, 903986, 903988)"