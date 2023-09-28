
if __name__ == "__main__":

    import CarCrashAnalysis
    from pyspark.sql import SparkSession
    from configparser import ConfigParser


    
    
    
    config = ConfigParser()
    config.read("config.ini")
    app_name=config.get("Spark","spark.app.name")
    spark = SparkSession.builder.appName(app_name).getOrCreate()



    analysis = CarCrashAnalysis(spark, config)
    result = analysis.Question_1()
    print("Q1) Number of crashes in which the person killed is male.")
    print(f"Number crashes with male victims killed: {result}")
    

    analysis = CarCrashAnalysis(spark, config)
    result = analysis.Question_2()
    print("Q2)How many two wheelers are booked for crashes? ")
    print(f"2-wheeler count: {result}")


    analysis = CarCrashAnalysis(spark, config)
    df_result = analysis.Question_3()
    print("Q3)Which state has highest number of accidents in which females are involved?")
    df_result.show()



    analysis = CarCrashAnalysis(spark, config)
    df_result = analysis.Question_4()
    print("Q4)Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death?")
    df_result.show()



    analysis = CarCrashAnalysis(spark, config)
    df_result = analysis.Question_5()
    print("Q5)For all the body styles involved in crashes, mention the top ethnic user group of each unique body style .")
    df_result.show()


    analysis = CarCrashAnalysis(spark, config)
    df_result = analysis.Question_6()
    print("Q6)Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code).")
    df_result.show()


    analysis = CarCrashAnalysis(spark, config)
    result = analysis.Question_7()
    print("Q7)Count of Distinct Crash IDs where  ‘No Damaged Property’ was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails insurance.")
    print("answer :",result)


    analysis = CarCrashAnalysis(spark, config)
    df_result = analysis.Question_8()
    print("Q8)Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offenses, has licensed Drivers, used top 10 used vehicle colors and has car licensed with the Top 25 states with highest number of offence")
    df_result.show()







    spark.stop()
