# spandex

## Spatial Analysis and Data Exploration

Data processing and imputation steps for UrbanSim.  Transform messy urban data into spatial insight!

Spandex is a library of functions that operate on pandas dataframes and spatial data.  Also includes functions for data loading from common data sources. A sequence of spandex functions can form a data ingestion/cleaning/imputation pipeline.  The spandex library aspires to give urban modelers the power to compose complex, powerful data cleaning pipelines with succinct, readable code.  Once an interoperable set of core functions is in place, they can be applied and composed together in a variety of different ways depending on the needs of each region and model.  

Every region developing the data for an UrbanSim model goes through a similar process.  Large amounts of raw data must be processed into a form usable for model estimation and simulation (i.e. an ETL step).  Most data problems encountered are common across regions, and present solutions are often ad hoc and difficult to replicate.  As part of the spandex library, we propose to develop an approach to assembling and cleaning urban data for modeling that will be generally useful.  Automating and streamlining the data processing stage is the first step towards an end-to-end urban analytics pipeline:  process data, estimate models, calibrate/validate models, make predictions. 

The spandex library consists of functions for conveniently:
* Loading common data formats
* Cleaning and correcting geometry
* Applying spatial operations typical in UrbanSim data development
* Applying data imputation operations typical in UrbanSim data development
* Auto-detection and correction of errors
* Data verification/QA/validation/reconciliation
* Export to UrbanSim HDF5, synchronize with UrbanCanvas database, export to other common formats

Motivating principles include:
* The ability to reproduce results should be a key element of an urban modeling workflow. All data manipulation and analysis should be repeatable and automated.
* The process used to generate UrbanSim data should be fully transparent
* Assumptions/choices in the data processing stage of model development influence model results, so data regeneration should be considered part of the modeling.

By facilitating end-to-end automation of data processing and cleaning steps, spandex will facilitate a workflow that is in accordance with best practices for reproducible research.   The resulting urban modeling data workflow will be more transparent and less error prone.   The random number generator seed used as part of each regeneration will be saved (since certain allocation steps will have a random component), along with the git version of code used.  Testing will be emphasized to ensure that code is functioning as expected.  The data pipeline will always be run from raw data so as to preserve the input in an unaltered state and so that all manipulations, from the earliest data processing steps onwards, are repeatable.  Data cleaning methodology influences model results and should be considered part of the modeling. 
 

