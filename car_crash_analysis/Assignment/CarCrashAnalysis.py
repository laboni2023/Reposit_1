#question8
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, monotonically_increasing_id,dense_rank
from pyspark.sql.window import Window
from configparser import ConfigParser






class CarCrashAnalysis:
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config




    def load_data(self, data_key):
        data_path = self.config.get("Data",data_key)
        df = self.spark.read.format("csv") \
            .option("inferschema", True) \
            .option("header", True) \
            .load(data_path)
        return df







    def Question_8(self):
        # Load data
        df_prim_person = self.load_data("data.primary_person")
        df_charges = self.load_data("data.charges")
        df_units = self.load_data("data.input.units")
        df_restrict = self.load_data("data.restrict")

        
        df_temp1=df_charges.filter(col("CHARGE").like("%SPEED%")).orderBy("CRASH_ID","UNIT_NBR","PRSN_NBR","CHARGE").select("CRASH_ID","UNIT_NBR","PRSN_NBR","CHARGE")
        df_temp2=df_units.filter((col("VEH_COLOR_ID")!="99") & (col("VEH_COLOR_ID")!="98")&(col("VEH_COLOR_ID")!="NA"))
        list_top10_colors=df_temp2.groupBy("VEH_COLOR_ID").count().orderBy(desc("count")).select("VEH_COLOR_ID").limit(10).rdd.flatMap(lambda x:x).collect()
        df_temp=df_charges.join(df_prim_person,(df_charges.CRASH_ID==df_prim_person.CRASH_ID)&(df_charges.UNIT_NBR==df_prim_person.UNIT_NBR)&(df_charges.PRSN_NBR==df_prim_person.PRSN_NBR))
        df_temp=df_temp.filter(col("DRVR_LIC_STATE_ID")!="NA").filter(col("DRVR_LIC_STATE_ID")!="Unknown").filter(col("DRVR_LIC_STATE_ID")!="Other")
        list_top25_states=df_temp.groupBy("DRVR_LIC_STATE_ID").count().orderBy(desc("count")).select("DRVR_LIC_STATE_ID").limit(25).rdd.flatMap(lambda x: x).collect()
        list_top25_states = [
            'TX', 'MX', 'LA', 'NM', 'CA', 'FL', 'OK', 'AR', 'AZ', 'GA', 'IL', 'MS', 'CO', 'NC', 'TN',
            'MO', 'AL', 'KS', 'OH', 'MI', 'NY', 'VA', 'NV', 'WA', 'SC'
        ]
        df_temp3=df_restrict.filter(col("DRVR_LIC_RESTRIC_ID")!="UNLICENSED")
        df_temp2=df_units.filter(col("VEH_MAKE_ID")!="NA").filter(col("VEH_MAKE_ID")!="UNKNOWN")
        df_temp1=df_temp1.withColumnRenamed("CRASH_ID","crash_id_2").withColumnRenamed("UNIT_NBR","unit_nbr_2")
        df_temp4=df_temp1.join(df_temp3,(df_temp1.crash_id_2==df_temp3.CRASH_ID)&(df_temp1.unit_nbr_2==df_temp3.UNIT_NBR))
        df_temp5=df_temp4.join(df_temp2,(df_temp4.CRASH_ID==df_temp2.CRASH_ID)&(df_temp4.UNIT_NBR==df_temp2.UNIT_NBR))        
        df_temp6=df_temp5.filter(col("VEH_COLOR_ID").isin(list_top10_colors)).filter(col("VEH_LIC_STATE_ID").isin(list_top25_states))
        df_result=df_temp6.groupBy("VEH_MAKE_ID").count().orderBy(desc("count")).withColumn("rank",monotonically_increasing_id()+1).select("VEH_MAKE_ID","rank")
        return df_result
    


    
    def Question_7(self):

        # Load data
        df_damages =  self.load_data("data.damages")
        df_units = self.load_data("data.input.units")

        df_temp1=df_damages.filter(col("DAMAGED_PROPERTY")=="NONE")
        df_temp2=df_units.filter((col("VEH_DMAG_SCL_1_ID")=="DAMAGED 5" )|(col("VEH_DMAG_SCL_1_ID")=="DAMAGED 6")|(col("VEH_DMAG_SCL_2_ID")=="DAMAGED 5" )|(col("VEH_DMAG_SCL_2_ID")=="DAMAGED 6" ))\
                    .filter(col("VEH_BODY_STYL_ID").like("%CAR%"))
        result = df_temp2.join(df_temp1,(df_temp2.CRASH_ID==df_temp1.CRASH_ID))\
            .select(df_temp1.CRASH_ID,"VEH_DMAG_SCL_1_ID").distinct().count()

        return result
    



    

    def Question_6(self):

        # Load data
        df_units = self.load_data("data.input.units")
        df_prim_person = self.load_data("data.primary_person")

        df_temp1=df_prim_person.filter((col("PRSN_ALC_RSLT_ID")=="Positive") & (col("DRVR_ZIP")!="null"))
        df_temp1=df_temp1.withColumnRenamed("CRASH_ID","crash_id2").withColumnRenamed("UNIT_NBR","unit_nbr2")
        df_temp2=df_temp1.join(df_units,(df_temp1.crash_id2==df_units.CRASH_ID)&(df_temp1.unit_nbr2==df_units.UNIT_NBR))
        df_temp3=df_temp2.filter(col("VEH_BODY_STYL_ID").like("%CAR%"))
        df_temp4=df_temp3.select("CRASH_ID","DRVR_ZIP").distinct().groupBy("DRVR_ZIP").count()
        df_result=df_temp4.orderBy(desc("count")).withColumnRenamed("count","count_of_crashes")

        return df_result
    




    def Question_5(self):

        # Load data
        df_prim_person = self.load_data("data.primary_person")
        df_charges = self.load_data("data.charges")
        df_units = self.load_data("data.input.units")


        df_temp1=df_prim_person.join(df_units,(df_prim_person.CRASH_ID==df_units.CRASH_ID) \
        & (df_prim_person.UNIT_NBR==df_units.UNIT_NBR)  ).select(df_units.CRASH_ID,df_units.UNIT_NBR,"VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")
        df_temp2=df_temp1.filter((col("VEH_BODY_STYL_ID")!="NA") & (col("PRSN_ETHNICITY_ID")!="NA"))\
            .filter((col("VEH_BODY_STYL_ID")!="UNKNOWN") & (col("PRSN_ETHNICITY_ID")!="UNKNOWN"))
        df_temp3=df_temp2.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count()
        window=Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("count"))
        df_temp4=df_temp3.withColumn("rank",dense_rank().over(window))
        df_result=df_temp4.filter(col("rank")==1).select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")

        return df_result
    





    def Question_4(self):

        # Load data
        df_units = self.load_data("data.input.units")

        df_temp1=df_units.groupBy("VEH_MAKE_ID").sum("TOT_INJRY_CNT","DEATH_CNT")\
            .withColumn("total_injuries_including_death",col("sum(TOT_INJRY_CNT)")+col("sum(DEATH_CNT)"))\
                .select("VEH_MAKE_ID","total_injuries_including_death")
        df_temp2=df_temp1.orderBy(desc("total_injuries_including_death")).filter(col("VEH_MAKE_ID")!="NA")
        df_result=df_temp2.withColumn("rank",monotonically_increasing_id()+1).filter((col("rank") >=5 ) &(col("rank") <=15 ))\
            .select("rank","VEH_MAKE_ID").show()
        
        return df_result
    




    def Question_3(self):

        # Load data
        df_prim_person = self.load_data("data.primary_person")

        df_temp1 = df_prim_person.filter(col("PRSN_GNDR_ID") == "FEMALE") \
            .filter(col("DRVR_LIC_STATE_ID") != "NA")
        df_temp2 = df_temp1.select("DRVR_LIC_STATE_ID", "CRASH_ID").distinct() \
            .groupBy("DRVR_LIC_STATE_ID").count()
        df_result = df_temp2.orderBy(desc("count")).withColumnRenamed("count", "count of crashes").limit(1)

        return df_result
    

    def Question_2(self):

        # Load data
        df_units = self.load_data("data.input.units")

        result = df_units.filter(df_units.VEH_BODY_STYL_ID.like("%MOTORCYCLE%")).count()

        return result
    


    def Question_1(self):

        # Load data
        df_prim_person = self.load_data()


        df_temp1 = df_prim_person.filter(
            (col("PRSN_INJRY_SEV_ID") == "KILLED") &
            (col("PRSN_GNDR_ID") == "MALE")
        )
        result = df_temp1.select("CRASH_ID").distinct().count()

        return result










