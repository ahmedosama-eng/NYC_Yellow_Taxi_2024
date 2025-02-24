from Extract.data_reader import reading_the_files_in_df,reading_the_lookups
from DWH_piplines import pipline_for_DWH
from LLM_pipline import pipline_for_LLM
from config import spark,data,lookups

if __name__=='__main__':    
    # reading from local DataLack
    #data=reading_the_files_in_df(spark)
    #lookups=reading_the_lookups(spark)
    
    #pipline_for_DWH(data,lookups,spark)

    pipline_for_LLM(data,lookups,spark)
    



    

    









