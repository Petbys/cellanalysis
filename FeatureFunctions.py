import pandas as pd
import re
import numpy as np

# create dictionary of dataframes of nuclei, cells and cytoplasm features
def cp_Features_extract(df_cp_results, OutputDir,plateNamePrefix):
    for index, oneplate_analysis in df_cp_results.iterrows():
        one_plate_filename = f'{ OutputDir }/{plateNamePrefix}_{ oneplate_analysis["plate_acq_name"] }.parquet'
        DataFrameDictionary ={}
        featureFileNames = ['featICF_nuclei','featICF_cells','featICF_cytoplasm']
        for featName in featureFileNames:
            DataFrameDictionary[featName] = pd.DataFrame()
            file = oneplate_analysis['results'] + featName + '.parquet'
            DataFromOneFile =  pd.read_parquet(file)
            DataFrameDictionary[featName] = DataFromOneFile
            # Add ( _nuclei, _cells, _cytoplasm suffix to column names
            DataFrameDictionary[featName].columns = [str(col) + '_' + re.sub('_.*', '', re.sub('featICF_', '', featName)) for col in DataFrameDictionary[featName]]
    return DataFrameDictionary,one_plate_filename

def merge_dataframes(df_1,df_2):
    df = df_1.merge(df_2, left_on = [ 'Metadata_Barcode_nuclei',
    'Metadata_Site_nuclei', 'Metadata_Well_nuclei','Parent_cells_nuclei'],
                       right_on = [ 'Metadata_Barcode_cells',
    'Metadata_Site_cells', 'Metadata_Well_cells','ObjectNumber_cells'], how='left')
    return df
# merge nuclei and cells
# merge_dataframes(merge_dataframes(DataFrameDictionary['featICF_nuclei'],DataFrameDictionary['featICF_cells'],DataFrameDictionary['featICF_cytoplasm']))

def rename_elements(df,one_plate_filename,aggregateFunction):
    df.rename(columns = {'Metadata_Barcode_nuclei':'Metadata_Barcode',
                         'Metadata_Well_nuclei':'Metadata_Well',
                         'Metadata_Site_nuclei':'Metadata_Site',
                         'Metadata_AcqID_nuclei':'Metadata_AcqID'}, inplace = True)
     # Create an ImageId column by merging <Barcode>_<Well>_<Site>
    df['ImageID'] =  df['Metadata_AcqID'].astype(str) + '_' + df['Metadata_Barcode'] + '_' + df['Metadata_Well'] + '_' + df['Metadata_Site'].astype(str)
    df.select_dtypes(include=np.number) 
    numeric_columns = df.select_dtypes(include=np.number).columns.tolist()#
    dictOfnumericColsAggregationFunctions = { i : aggregateFunction for i in numeric_columns}
    groupedbyImage = df.groupby(['ImageID','Metadata_Barcode','Metadata_Well', 'Metadata_Site','Metadata_AcqID'], as_index=False).agg(dictOfnumericColsAggregationFunctions)
    groupedbyImage.to_parquet( one_plate_filename )
    


def Group_Images_AllPlates(df_cp_results,OutputDir,plateNamePrefix):
    groupedbyImageAllPlates = pd.DataFrame()
    for index, oneplate_analysis_meta in df_cp_results.iterrows(): 
        one_plate_filename = f'{ OutputDir }/{plateNamePrefix}_{ oneplate_analysis_meta["plate_acq_name"] }.parquet'
        print(f'read file: {one_plate_filename}')
        df = pd.read_parquet(one_plate_filename)
        print(f"df.shape(rows, cols): {df.shape}")
        groupedbyImageAllPlates = pd.concat([groupedbyImageAllPlates, df])
    return groupedbyImageAllPlates
def save_all(groupedbyImageAllPlates,OutputDir,plateNamePrefix):
    all_plates_outfile = f'{OutputDir}/{plateNamePrefix}AllPlates.parquet'
    groupedbyImageAllPlates.to_parquet(all_plates_outfile)    


#OutputDir = 'ImageMedianFeatures_2022_11_22' # Change accordingly
#aggregateFunction = np.nanmedian
#plateNamePrefix = 'ImageMedianPlate'

